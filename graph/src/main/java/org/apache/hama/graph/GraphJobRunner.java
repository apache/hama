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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;

/**
 * Fully generic graph job runner.
 * 
 * @param <V> the id type of a vertex.
 * @param <E> the value type of an edge.
 * @param <M> the value type of a vertex.
 */
public final class GraphJobRunner<V extends WritableComparable<V>, E extends Writable, M extends Writable>
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
  public static Class<? extends WritableComparable<?>> VERTEX_ID_CLASS;
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
      final Map<V, List<M>> messages = parseMessages(peer);
      // master needs to update
      doMasterUpdates(peer);
      // if aggregators say we don't have updates anymore, break
      if (!aggregationRunner.receiveAggregatedValues(peer, iteration)) {
        break;
      }
      // loop over vertices and do their computation
      doSuperstep(messages, peer);

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
    for (Vertex<V, E, M> e : vertices) {
      peer.write(e.getVertexID(), e.getValue());
    }
  }

  /**
   * The master task is going to check the number of updated vertices and do
   * master aggregation. In case of no aggregators defined, we save a sync by
   * reading multiple typed messages.
   */
  private void doMasterUpdates(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    if (isMasterTask(peer) && iteration > 1) {
      MapWritable updatedCnt = new MapWritable();
      // exit if there's no update made
      if (globalUpdateCounts == 0) {
        updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(Integer.MIN_VALUE));
      } else {
        aggregationRunner.doMasterAggregation(updatedCnt);
      }
      // send the updates from the mater tasks back to the slaves
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new GraphJobMessage(updatedCnt));
      }
    }
  }

  /**
   * Do the main logic of a superstep, namely checking if vertices are active,
   * feeding compute with messages and controlling combiners/aggregators.
   */
  private void doSuperstep(Map<V, List<M>> messages,
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    int activeVertices = 0;
    for (Vertex<V, E, M> vertex : vertices) {
      List<M> msgs = messages.get(vertex.getVertexID());
      // If there are newly received messages, restart.
      if (vertex.isHalted() && msgs != null) {
        vertex.setActive();
      }
      if (msgs == null) {
        msgs = Collections.emptyList();
      }

      if (!vertex.isHalted()) {
        if (combiner != null) {
          M combined = combiner.combine(msgs);
          msgs = new ArrayList<M>();
          msgs.add(combined);
        }
        M lastValue = vertex.getValue();
        vertex.compute(msgs.iterator());
        aggregationRunner.aggregateVertex(lastValue, vertex);
        if (!vertex.isHalted()) {
          activeVertices++;
        }
      }
    }

    aggregationRunner.sendAggregatorValues(peer, activeVertices);
    iteration++;
  }

  /**
   * Seed the vertices first with their own values in compute. This is the first
   * superstep after the vertices have been loaded.
   */
  private void doInitialSuperstep(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    for (Vertex<V, E, M> vertex : vertices) {
      List<M> singletonList = Collections.singletonList(vertex.getValue());
      M lastValue = vertex.getValue();
      vertex.compute(singletonList.iterator());
      aggregationRunner.aggregateVertex(lastValue, vertex);
    }
    aggregationRunner.sendAggregatorValues(peer, 1);
    iteration++;
  }

  @SuppressWarnings("unchecked")
  private void setupFields(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    this.peer = peer;
    this.conf = peer.getConfiguration();
    maxIteration = peer.getConfiguration().getInt("hama.graph.max.iteration",
        -1);

    initClasses(conf);

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

    aggregationRunner = new AggregationRunner<V, E, M>();
    aggregationRunner.setupAggregators(peer);

    vertices = new VerticesInfo<V, E, M>();
  }

  @SuppressWarnings("unchecked")
  public static <V extends WritableComparable<V>, E extends Writable, M extends Writable> void initClasses(
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
  @SuppressWarnings("unchecked")
  private void loadVertices(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    final boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);

    if (LOG.isDebugEnabled())
      LOG.debug("Vertex class: " + vertexClass);

    KeyValuePair<Writable, Writable> next;
    while ((next = peer.readNext()) != null) {
      Vertex<V, E, M> vertex = (Vertex<V, E, M>) next.getKey();
      vertex.runner = this;
      vertex.setup(conf);
      vertices.addVertex(vertex);
      if (selfReference) {
        vertex.addEdge(new Edge<V, E>(vertex.getVertexID(), null));
      }
    }

    LOG.info(vertices.size() + " vertices are loaded into "
        + peer.getPeerName());

    if (LOG.isDebugEnabled())
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
   * @return a map that contains messages pro vertex.
   */
  @SuppressWarnings("unchecked")
  private Map<V, List<M>> parseMessages(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    GraphJobMessage msg;
    final Map<V, List<M>> msgMap = new HashMap<V, List<M>>();
    while ((msg = peer.getCurrentMessage()) != null) {
      // either this is a vertex message or a directive that must be read
      // as map
      if (msg.isVertexMessage()) {
        final V vertexID = (V) msg.getVertexId();
        final M value = (M) msg.getVertexValue();
        List<M> msgs = msgMap.get(vertexID);
        if (msgs == null) {
          msgs = new ArrayList<M>();
          msgMap.put(vertexID, msgs);
        }
        msgs.add(value);
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
    return msgMap;
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
  public static <V extends WritableComparable<? super V>, E extends Writable, M extends Writable> Vertex<V, E, M> newVertexInstance(
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
