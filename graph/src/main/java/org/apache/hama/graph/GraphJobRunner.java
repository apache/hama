/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.graph;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.graph.IDSkippingIterator.Strategy;
import org.apache.hama.util.ReflectionUtils;

/**
 * Fully generic graph job runner.
 * 
 * @param <V> the id type of a vertex.
 * @param <E> the value type of an edge.
 * @param <M> the value type of a vertex.
 */
@SuppressWarnings("rawtypes")
public final class GraphJobRunner<V extends WritableComparable, E extends Writable, M extends Writable>
    extends BSP<Writable, Writable, Writable, Writable, GraphJobMessage> {

  public static enum GraphJobCounter {
    MULTISTEP_PARTITIONING, ITERATIONS, INPUT_VERTICES, AGGREGATE_VERTICES
  }

  private static final Log LOG = LogFactory.getLog(GraphJobRunner.class);

  // make sure that these values don't collide with the vertex names
  public static final String S_FLAG_MESSAGE_COUNTS = "hama.0";
  public static final String S_FLAG_AGGREGATOR_VALUE = "hama.1";
  public static final String S_FLAG_AGGREGATOR_INCREMENT = "hama.2";
  public static final Text FLAG_MESSAGE_COUNTS = new Text(S_FLAG_MESSAGE_COUNTS);

  public static final String MESSAGE_COMBINER_CLASS_KEY = "hama.vertex.message.combiner.class";
  public static final String VERTEX_CLASS_KEY = "hama.graph.vertex.class";

  private Configuration conf;
  private Combiner<M> combiner;
  private Partitioner<V, M> partitioner;

  public static Class<?> VERTEX_CLASS;
  public static Class<? extends WritableComparable> VERTEX_ID_CLASS;
  public static Class<? extends Writable> VERTEX_VALUE_CLASS;
  public static Class<? extends Writable> EDGE_VALUE_CLASS;
  public static Class<Vertex<?, ?, ?>> vertexClass;

  private VerticesInfo<V, E, M> vertices;
  private boolean updated = true;
  private int globalUpdateCounts = 0;

  private long numberVertices = 0;
  // -1 is deactivated
  private int maxIteration = -1;
  private long iteration;

  private AggregationRunner<V, E, M> aggregationRunner;
  private VertexOutputWriter<Writable, Writable, V, E, M> vertexOutputWriter;

  private BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer;

  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {

    setupFields(peer);

    loadVertices(peer);

    countGlobalVertexCount(peer);

    doInitialSuperstep(peer);

  }

  @Override
  public final void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {

    // we do supersteps while we still have updates and have not reached our
    // maximum iterations yet
    while (updated && !((maxIteration > 0) && iteration > maxIteration)) {
      // reset the global update counter from our master in every
      // superstep
      globalUpdateCounts = 0;
      peer.sync();

      // note that the messages must be parsed here
      GraphJobMessage firstVertexMessage = parseMessages(peer);
      // master/slaves needs to update
      firstVertexMessage = doAggregationUpdates(firstVertexMessage, peer);
      // check if updated changed by our aggregators
      if (!updated) {
        break;
      }

      // loop over vertices and do their computation
      doSuperstep(firstVertexMessage, peer);

      if (isMasterTask(peer)) {
        peer.getCounter(GraphJobCounter.ITERATIONS).increment(1);
      }
    }

  }

  /**
   * Just write <ID as Writable, Value as Writable> pair as a result. Note that
   * this will also be executed when failure happened.
   */
  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    vertexOutputWriter.setup(conf);
    IDSkippingIterator<V, E, M> skippingIterator = vertices.skippingIterator();
    while (skippingIterator.hasNext()) {
      vertexOutputWriter.write(skippingIterator.next(), peer);
    }
    vertices.cleanup(conf, peer.getTaskId());
  }

  /**
   * The master task is going to check the number of updated vertices and do
   * master aggregation. In case of no aggregators defined, we save a sync by
   * reading multiple typed messages.
   */
  private GraphJobMessage doAggregationUpdates(
      GraphJobMessage firstVertexMessage,
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {

    // this is only done in every second iteration
    if (isMasterTask(peer) && iteration > 1) {
      MapWritable updatedCnt = new MapWritable();
      // exit if there's no update made
      if (globalUpdateCounts == 0) {
        updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(Integer.MIN_VALUE));
      } else {
        aggregationRunner.doMasterAggregation(updatedCnt);
      }
      // send the updates from the master tasks back to the slaves
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new GraphJobMessage(updatedCnt));
      }
    }
    if (aggregationRunner.isEnabled() && iteration > 1) {
      // in case we need to sync, we need to replay the messages that already
      // are added to the queue. This prevents loosing messages when using
      // aggregators.
      if (firstVertexMessage != null) {
        peer.send(peer.getPeerName(), firstVertexMessage);
      }
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        peer.send(peer.getPeerName(), msg);
      }
      // now sync
      peer.sync();
      // now the map message must be read that might be send from the master
      updated = aggregationRunner.receiveAggregatedValues(peer
          .getCurrentMessage().getMap(), iteration);
      // set the first vertex message back to the message it had before sync
      firstVertexMessage = peer.getCurrentMessage();
    }
    return firstVertexMessage;
  }

  /**
   * Do the main logic of a superstep, namely checking if vertices are active,
   * feeding compute with messages and controlling combiners/aggregators.
   */
  @SuppressWarnings("unchecked")
  private void doSuperstep(GraphJobMessage currentMessage,
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    int activeVertices = 0;
    vertices.startSuperstep();
    /*
     * We iterate over our messages and vertices in sorted order. That means
     * that we need to seek the first vertex that has the same ID as the
     * currentMessage or the first vertex that is active.
     */
    IDSkippingIterator<V, E, M> iterator = vertices.skippingIterator();
    // note that can't skip inactive vertices because we have to rewrite the
    // complete vertex file in each iteration
    while (iterator.hasNext(
        currentMessage == null ? null : (V) currentMessage.getVertexId(),
        Strategy.ALL)) {

      Vertex<V, E, M> vertex = iterator.next();
      VertexMessageIterable<V, M> iterable = null;
      if (currentMessage != null) {
        iterable = iterate(currentMessage, (V) currentMessage.getVertexId(),
            vertex, peer);
      }
      if (iterable != null && vertex.isHalted()) {
        vertex.setActive();
      }
      if (!vertex.isHalted()) {
        M lastValue = vertex.getValue();
        if (iterable == null) {
          vertex.compute(Collections.<M> emptyList());
        } else {
          if (combiner != null) {
            M combined = combiner.combine(iterable);
            vertex.compute(Collections.singleton(combined));
          } else {
            vertex.compute(iterable);
          }
          currentMessage = iterable.getOverflowMessage();
        }
        aggregationRunner.aggregateVertex(lastValue, vertex);
        // check for halt again after computation
        if (!vertex.isHalted()) {
          activeVertices++;
        }
      }

      // note that we even need to rewrite the vertex if it is halted for
      // consistency reasons
      vertices.finishVertexComputation(vertex);
    }
    vertices.finishSuperstep();

    aggregationRunner.sendAggregatorValues(peer, activeVertices);
    iteration++;
  }

  /**
   * Iterating utility that ensures following things: <br/>
   * - if vertex is active, but the given message does not match the vertexID,
   * return null. <br/>
   * - if vertex is inactive, but received a message that matches the ID, build
   * an iterator that can be iterated until the next vertex has been reached
   * (not buffer in memory) and set the vertex active <br/>
   * - if vertex is active, and the given message does match the vertexID,
   * return an iterator that can be iterated until the next vertex has been
   * reached. <br/>
   * - if vertex is inactive, and received no message, return null.
   */
  private VertexMessageIterable<V, M> iterate(GraphJobMessage currentMessage,
      V firstMessageId, Vertex<V, E, M> vertex,
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    @SuppressWarnings("unchecked")
    int comparision = firstMessageId.compareTo(vertex.getVertexID());
    if (comparision < 0) {
      throw new IllegalArgumentException(
          "Messages must never be behind the vertex in ID! Current Message ID: "
              + firstMessageId + " vs. " + vertex.getVertexID());
    } else if (comparision == 0) {
      // vertex id matches with the vertex, return an iterator with newest
      // message
      return new VertexMessageIterable<V, M>(currentMessage,
          vertex.getVertexID(), peer);
    } else {
      // return null
      return null;
    }
  }

  /**
   * Seed the vertices first with their own values in compute. This is the first
   * superstep after the vertices have been loaded.
   */
  private void doInitialSuperstep(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    vertices.startSuperstep();
    IDSkippingIterator<V, E, M> skippingIterator = vertices.skippingIterator();
    while (skippingIterator.hasNext()) {
      Vertex<V, E, M> vertex = skippingIterator.next();
      M lastValue = vertex.getValue();
      vertex.compute(Collections.singleton(vertex.getValue()));
      aggregationRunner.aggregateVertex(lastValue, vertex);
      vertices.finishVertexComputation(vertex);
    }
    vertices.finishSuperstep();
    aggregationRunner.sendAggregatorValues(peer, 1);
    iteration++;
  }

  @SuppressWarnings("unchecked")
  private void setupFields(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    this.peer = peer;
    this.conf = peer.getConfiguration();
    maxIteration = peer.getConfiguration().getInt("hama.graph.max.iteration",
        -1);

    GraphJobRunner.<V, E, M> initClasses(conf);

    partitioner = (Partitioner<V, M>) org.apache.hadoop.util.ReflectionUtils
        .newInstance(
            conf.getClass("bsp.input.partitioner.class", HashPartitioner.class),
            conf);

    if (!conf.getClass(MESSAGE_COMBINER_CLASS_KEY, Combiner.class).equals(
        Combiner.class)) {
      LOG.debug("Combiner class: " + conf.get(MESSAGE_COMBINER_CLASS_KEY));

      combiner = (Combiner<M>) org.apache.hadoop.util.ReflectionUtils
          .newInstance(conf.getClass("hama.vertex.message.combiner.class",
              Combiner.class), conf);
    }

    Class<?> outputWriter = conf.getClass(
        GraphJob.VERTEX_OUTPUT_WRITER_CLASS_ATTR, VertexOutputWriter.class);
    vertexOutputWriter = (VertexOutputWriter<Writable, Writable, V, E, M>) ReflectionUtils
        .newInstance(outputWriter);

    aggregationRunner = new AggregationRunner<V, E, M>();
    aggregationRunner.setupAggregators(peer);

    // FIXME We should make this configurable.
    vertices = new ListVerticesInfo();
    vertices.init(this, conf, peer.getTaskId());
  }

  @SuppressWarnings("unchecked")
  public static <V extends WritableComparable<? super V>, E extends Writable, M extends Writable> void initClasses(
      Configuration conf) {
    Class<V> vertexIdClass = (Class<V>) conf.getClass(
        GraphJob.VERTEX_ID_CLASS_ATTR, Text.class, Writable.class);
    Class<M> vertexValueClass = (Class<M>) conf.getClass(
        GraphJob.VERTEX_VALUE_CLASS_ATTR, IntWritable.class, Writable.class);
    Class<E> edgeValueClass = (Class<E>) conf.getClass(
        GraphJob.VERTEX_EDGE_VALUE_CLASS_ATTR, IntWritable.class,
        Writable.class);
    vertexClass = (Class<Vertex<?, ?, ?>>) conf.getClass(
        "hama.graph.vertex.class", Vertex.class);

    // set the classes statically, so we can save memory per message
    VERTEX_ID_CLASS = vertexIdClass;
    VERTEX_VALUE_CLASS = vertexValueClass;
    VERTEX_CLASS = vertexClass;
    EDGE_VALUE_CLASS = edgeValueClass;
  }

  /**
   * Loads vertices into memory of each peer.
   */
  private void loadVertices(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    final boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);

    LOG.debug("Vertex class: " + vertexClass);

    // our VertexInputReader ensures incoming vertices are sorted by their ID
    Vertex<V, E, M> vertex = GraphJobRunner
        .<V, E, M> newVertexInstance(VERTEX_CLASS);
    vertex.runner = this;
    while (peer.readNext(vertex, NullWritable.get())) {
      vertex.setup(conf);
      if (selfReference) {
        vertex.addEdge(new Edge<V, E>(vertex.getVertexID(), null));
      }

      vertices.addVertex(vertex);

      // Reinitializing vertex object for memory based implementations of
      // VerticesInfo
      vertex = GraphJobRunner.<V, E, M> newVertexInstance(VERTEX_CLASS);
      vertex.runner = this;
    }
    vertices.finishAdditions();
    // finish the "superstep" because we have written a new file here
    vertices.finishSuperstep();

    LOG.info(vertices.size() + " vertices are loaded into "
        + peer.getPeerName());
    LOG.debug("Starting Vertex processing!");
  }

  /**
   * Counts vertices globally by sending the count of vertices in the map to the
   * other peers.
   */
  private void countGlobalVertexCount(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, new GraphJobMessage(new IntWritable(vertices.size())));
    }

    peer.sync();

    GraphJobMessage msg;
    while ((msg = peer.getCurrentMessage()) != null) {
      if (msg.isVerticesSizeMessage()) {
        numberVertices += msg.getVerticesSize().get();
      }
    }

    if (isMasterTask(peer)) {
      peer.getCounter(GraphJobCounter.INPUT_VERTICES).increment(numberVertices);
    }
  }

  /**
   * Parses the messages in every superstep and does actions according to flags
   * in the messages.
   * 
   * @return the first vertex message, null if none received.
   */
  @SuppressWarnings("unchecked")
  private GraphJobMessage parseMessages(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    GraphJobMessage msg = null;
    while ((msg = peer.getCurrentMessage()) != null) {
      // either this is a vertex message or a directive that must be read
      // as map
      if (msg.isVertexMessage()) {
        // if we found a vertex message (ordering defines they come after map
        // messages, we return that as the first message so the outward process
        // can join them correctly with the VerticesInfo.
        break;
      } else if (msg.isMapMessage()) {
        for (Entry<Writable, Writable> e : msg.getMap().entrySet()) {
          Text vertexID = (Text) e.getKey();
          if (FLAG_MESSAGE_COUNTS.equals(vertexID)) {
            if (((IntWritable) e.getValue()).get() == Integer.MIN_VALUE) {
              updated = false;
            } else {
              globalUpdateCounts += ((IntWritable) e.getValue()).get();
            }
          } else if (aggregationRunner.isEnabled()
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_VALUE)) {
            aggregationRunner.masterReadAggregatedValue(vertexID,
                (M) e.getValue());
          } else if (aggregationRunner.isEnabled()
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_INCREMENT)) {
            aggregationRunner.masterReadAggregatedIncrementalValue(vertexID,
                (M) e.getValue());
          }
        }

      } else {
        throw new UnsupportedOperationException("Unknown message type: " + msg);
      }

    }
    return msg;
  }

  /**
   * @return the number of vertices, globally accumulated.
   */
  public final long getNumberVertices() {
    return numberVertices;
  }

  /**
   * @return the current number of iterations.
   */
  public final long getNumberIterations() {
    return iteration;
  }

  /**
   * @return the defined number of maximum iterations, -1 if not defined.
   */
  public final int getMaxIteration() {
    return maxIteration;
  }

  /**
   * @return the defined partitioner instance.
   */
  public final Partitioner<V, M> getPartitioner() {
    return partitioner;
  }

  /**
   * Gets the last aggregated value at the given index. The index is dependend
   * on how the aggregators were configured during job setup phase.
   * 
   * @return the value of the aggregator, or null if none was defined.
   */
  public final Writable getLastAggregatedValue(int index) {
    return aggregationRunner.getLastAggregatedValue(index);
  }

  /**
   * Gets the last aggregated number of vertices at the given index. The index
   * is dependend on how the aggregators were configured during job setup phase.
   * 
   * @return the value of the aggregator, or null if none was defined.
   */
  public final IntWritable getNumLastAggregatedVertices(int index) {
    return aggregationRunner.getNumLastAggregatedVertices(index);
  }

  /**
   * @return the peer instance.
   */
  public final BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> getPeer() {
    return peer;
  }

  /**
   * Checks if this is a master task. The master task is the first peer in the
   * peer array.
   */
  public static boolean isMasterTask(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    return peer.getPeerName().equals(getMasterTask(peer));
  }

  /**
   * @return the name of the master peer, the name at the first index of the
   *         peers.
   */
  public static String getMasterTask(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    return peer.getPeerName(0);
  }

  /**
   * @return a new vertex instance
   */
  @SuppressWarnings({ "unchecked" })
  public static <V extends WritableComparable, E extends Writable, M extends Writable> Vertex<V, E, M> newVertexInstance(
      Class<?> vertexClass) {
    return (Vertex<V, E, M>) ReflectionUtils.newInstance(vertexClass);
  }

  // following new instances don't need conf injects.

  /**
   * @return a new vertex id object.
   */
  @SuppressWarnings("unchecked")
  public static <X extends Writable> X createVertexIDObject() {
    return (X) ReflectionUtils.newInstance(VERTEX_ID_CLASS);
  }

  /**
   * @return a new vertex value object.
   */
  @SuppressWarnings("unchecked")
  public static <X extends Writable> X createVertexValue() {
    return (X) ReflectionUtils.newInstance(VERTEX_VALUE_CLASS);
  }

  /**
   * @return a new edge cost object.
   */
  @SuppressWarnings("unchecked")
  public static <X extends Writable> X createEdgeCostObject() {
    return (X) ReflectionUtils.newInstance(EDGE_VALUE_CLASS);
  }

}
