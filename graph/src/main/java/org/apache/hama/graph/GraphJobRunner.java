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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
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
  public static final String S_FLAG_VERTEX_INCREASE = "hama.3";
  public static final String S_FLAG_VERTEX_DECREASE = "hama.4";
  public static final String S_FLAG_VERTEX_ALTER_COUNTER = "hama.5";
  public static final String S_FLAG_VERTEX_TOTAL_VERTICES = "hama.6";
  public static final Text FLAG_MESSAGE_COUNTS = new Text(S_FLAG_MESSAGE_COUNTS);
  public static final Text FLAG_VERTEX_INCREASE = new Text(
      S_FLAG_VERTEX_INCREASE);
  public static final Text FLAG_VERTEX_DECREASE = new Text(
      S_FLAG_VERTEX_DECREASE);
  public static final Text FLAG_VERTEX_ALTER_COUNTER = new Text(
      S_FLAG_VERTEX_ALTER_COUNTER);
  public static final Text FLAG_VERTEX_TOTAL_VERTICES = new Text(
      S_FLAG_VERTEX_TOTAL_VERTICES);

  public static final String VERTEX_CLASS_KEY = "hama.graph.vertex.class";

  private HamaConfiguration conf;
  private Partitioner<V, M> partitioner;

  public static Class<?> VERTEX_CLASS;
  public static Class<? extends WritableComparable> VERTEX_ID_CLASS;
  public static Class<? extends Writable> VERTEX_VALUE_CLASS;
  public static Class<? extends Writable> EDGE_VALUE_CLASS;
  public static Class<Vertex<?, ?, ?>> vertexClass;

  private VerticesInfo<V, E, M> vertices;

  private boolean updated = true;
  private int globalUpdateCounts = 0;
  private int changedVertexCnt = 0;

  private long numberVertices = 0;
  // -1 is deactivated
  private int maxIteration = -1;
  private long iteration = 0;

  private AggregationRunner<V, E, M> aggregationRunner;
  private VertexOutputWriter<Writable, Writable, V, E, M> vertexOutputWriter;

  private BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer;

  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {

    setupFields(peer);

    long startTime = System.currentTimeMillis();
    loadVertices(peer);
    LOG.info("Total time spent for loading vertices: "
        + (System.currentTimeMillis() - startTime) + " ms");

    startTime = System.currentTimeMillis();
    countGlobalVertexCount(peer);
    LOG.info("Total time spent for broadcasting global vertex count: "
        + (System.currentTimeMillis() - startTime) + " ms");

    startTime = System.currentTimeMillis();
    doInitialSuperstep(peer);
    LOG.info("Total time spent for initial superstep: "
        + (System.currentTimeMillis() - startTime) + " ms");
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

      long startTime = System.currentTimeMillis();
      // master/slaves needs to update
      doAggregationUpdates(peer);
      LOG.info("Total time spent for broadcasting aggregation values: "
          + (System.currentTimeMillis() - startTime) + " ms");

      // check if updated changed by our aggregators
      if (!updated) {
        break;
      }

      // loop over vertices and do their computation
      startTime = System.currentTimeMillis();
      doSuperstep(firstVertexMessage, peer);
      LOG.info("Total time spent for " + peer.getSuperstepCount()
          + " superstep: " + (System.currentTimeMillis() - startTime) + " ms");

      if (isMasterTask(peer)) {
        peer.getCounter(GraphJobCounter.ITERATIONS).increment(1);
      }
    }

  }

  /**
   * Just write <ID as Writable, Value as Writable> pair as a result. Note that
   * this will also be executed when failure happened.
   * 
   * @param peer
   * @throws java.io.IOException
   */
  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    vertexOutputWriter.setup(conf);
    Iterator<Vertex<V, E, M>> iterator = vertices.iterator();
    while (iterator.hasNext()) {
      vertexOutputWriter.write(iterator.next(), peer);
    }
    vertices.clear();
  }

  /**
   * The master task is going to check the number of updated vertices and do
   * master aggregation. In case of no aggregators defined, we save a sync by
   * reading multiple typed messages.
   */
  private void doAggregationUpdates(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {

    // this is only done in every second iteration
    if (isMasterTask(peer)) {
      MapWritable updatedCnt = new MapWritable();
      // send total number of vertices.
      updatedCnt.put(
          FLAG_VERTEX_TOTAL_VERTICES,
          new LongWritable((peer.getCounter(GraphJobCounter.INPUT_VERTICES)
              .getCounter())));
      // exit if there's no update made
      if (globalUpdateCounts == 0) {
        updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(Integer.MIN_VALUE));
      } else {
        getAggregationRunner().doMasterAggregation(updatedCnt);
      }
      // send the updates from the master tasks back to the slaves
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new GraphJobMessage(updatedCnt));
      }
    }

    if (getAggregationRunner().isEnabled()) {
      peer.sync();
      // now the map message must be read that might be send from the master
      updated = getAggregationRunner().receiveAggregatedValues(
          peer.getCurrentMessage().getMap(), iteration);
    }
  }

  /**
   * Do the main logic of a superstep, namely checking if vertices are active,
   * feeding compute with messages and controlling combiners/aggregators. We
   * iterate over our messages and vertices in sorted order. That means that we
   * need to seek the first vertex that has the same ID as the iterated message.
   */
  @SuppressWarnings("unchecked")
  private void doSuperstep(GraphJobMessage currentMessage,
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    this.changedVertexCnt = 0;
    vertices.startSuperstep();

    ExecutorService executor = Executors.newFixedThreadPool((peer
        .getNumCurrentMessages() / conf.getInt(
        "hama.graph.threadpool.percentage", 10)) + 1);

    while (currentMessage != null) {
      Runnable worker = new ComputeRunnable(vertices.get((V) currentMessage
          .getVertexId()), (Iterable<M>) currentMessage.getIterableMessages());
      executor.execute(worker);

      currentMessage = peer.getCurrentMessage();
    }

    executor.shutdown();
    while (!executor.isTerminated()) {
    }

    for (V v : vertices.getNotComputedVertices()) {
      if (!vertices.get(v).isHalted()) {
        Vertex<V, E, M> vertex = vertices.get(v);
        vertex.compute(Collections.<M> emptyList());
        vertices.finishVertexComputation(vertex);
      }
    }

    vertices.finishSuperstep();

    getAggregationRunner().sendAggregatorValues(peer,
        vertices.getComputedVertices().size(), this.changedVertexCnt);
    this.iteration++;
  }

  /**
   * Seed the vertices first with their own values in compute. This is the first
   * superstep after the vertices have been loaded.
   */
  private void doInitialSuperstep(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    this.changedVertexCnt = 0;
    vertices.startSuperstep();

    ExecutorService executor = Executors.newFixedThreadPool((vertices.size()
        / conf.getInt("hama.graph.threadpool.percentage", 10)) + 1);

    for (Vertex<V, E, M> v : vertices.getValues()) {
      Runnable worker = new ComputeRunnable(v, Collections.singleton(v
          .getValue()));
      executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
    }

    vertices.finishSuperstep();
    getAggregationRunner().sendAggregatorValues(peer, 1, this.changedVertexCnt);
    iteration++;
  }

  class ComputeRunnable implements Runnable {
    Vertex<V, E, M> vertex;
    Iterable<M> msgs;

    public ComputeRunnable(Vertex<V, E, M> vertex, Iterable<M> msgs) {
      this.vertex = vertex;
      this.msgs = msgs;
    }

    @Override
    public void run() {
      try {
        // call once at initial superstep
        if(iteration == 0)
          vertex.setup(conf);

        vertex.compute(msgs);
        vertices.finishVertexComputation(vertex);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
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

    Class<?> outputWriter = conf.getClass(
        GraphJob.VERTEX_OUTPUT_WRITER_CLASS_ATTR, VertexOutputWriter.class);
    vertexOutputWriter = (VertexOutputWriter<Writable, Writable, V, E, M>) ReflectionUtils
        .newInstance(outputWriter);

    setAggregationRunner(new AggregationRunner<V, E, M>());
    getAggregationRunner().setupAggregators(peer);

    Class<? extends VerticesInfo<V, E, M>> verticesInfoClass = (Class<? extends VerticesInfo<V, E, M>>) conf
        .getClass("hama.graph.vertices.info", MapVerticesInfo.class,
            VerticesInfo.class);
    vertices = ReflectionUtils.newInstance(verticesInfoClass);
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
  @SuppressWarnings("unchecked")
  private void loadVertices(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    VertexInputReader<Writable, Writable, V, E, M> reader = (VertexInputReader<Writable, Writable, V, E, M>) ReflectionUtils
        .newInstance(conf.getClass(Constants.RUNTIME_PARTITION_RECORDCONVERTER,
            VertexInputReader.class));

    ExecutorService executor = Executors.newFixedThreadPool(1000);

    try {
      KeyValuePair<Writable, Writable> next = null;
      while ((next = peer.readNext()) != null) {

        Vertex<V, E, M> vertex = GraphJobRunner
            .<V, E, M> newVertexInstance(VERTEX_CLASS);

        boolean vertexFinished = reader.parseVertex(next.getKey(),
            next.getValue(), vertex);
        if (!vertexFinished) {
          continue;
        }

        String dstHost = getHostName(vertex.getVertexID());
        if (peer.getPeerName().equals(dstHost)) {
          Runnable worker = new LoadWorker(vertex);
          executor.execute(worker);
        } else {
          peer.send(dstHost, new GraphJobMessage(vertex));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    peer.sync();

    GraphJobMessage msg;
    while ((msg = peer.getCurrentMessage()) != null) {
      Runnable worker = new LoadWorker((Vertex<V, E, M>) msg.getVertex());
      executor.execute(worker);
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
    }

    LOG.info(vertices.size() + " vertices are loaded into "
        + peer.getPeerName());
  }

  class LoadWorker implements Runnable {
    Vertex<V, E, M> vertex;

    public LoadWorker(Vertex<V, E, M> vertex) {
      this.vertex = vertex;
    }

    @Override
    public void run() {
      try {
        addVertex(vertex);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Add new vertex into memory of each peer.
   * 
   * @throws IOException
   */
  private void addVertex(Vertex<V, E, M> vertex) throws IOException {
    if (conf.getBoolean("hama.graph.self.ref", false)) {
      vertex.addEdge(new Edge<V, E>(vertex.getVertexID(), null));
    }

    vertex.setRunner(this);
    vertices.put(vertex);
  }

  /**
   * Remove vertex from this peer.
   * 
   * @throws IOException
   */
  private void removeVertex(V vertexID) {
    vertices.remove(vertexID);

    LOG.debug("Removed VertexID: " + vertexID + " in peer "
        + peer.getPeerName());
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
    boolean dynamicAdditions = false;
    boolean dynamicRemovals = false;

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
          } else if (getAggregationRunner().isEnabled()
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_VALUE)) {
            getAggregationRunner().masterReadAggregatedValue(vertexID,
                (M) e.getValue());
          } else if (getAggregationRunner().isEnabled()
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_INCREMENT)) {
            getAggregationRunner().masterReadAggregatedIncrementalValue(
                vertexID, (M) e.getValue());
          } else if (FLAG_VERTEX_INCREASE.equals(vertexID)) {
            dynamicAdditions = true;
            addVertex((Vertex<V, E, M>) e.getValue());
          } else if (FLAG_VERTEX_DECREASE.equals(vertexID)) {
            dynamicRemovals = true;
            removeVertex((V) e.getValue());
          } else if (FLAG_VERTEX_TOTAL_VERTICES.equals(vertexID)) {
            this.numberVertices = ((LongWritable) e.getValue()).get();
          } else if (FLAG_VERTEX_ALTER_COUNTER.equals(vertexID)) {
            if (isMasterTask(peer)) {
              peer.getCounter(GraphJobCounter.INPUT_VERTICES).increment(
                  ((LongWritable) e.getValue()).get());
            } else {
              throw new UnsupportedOperationException(
                  "A message to increase vertex count is in a wrong place: "
                      + peer);
            }
          }
        }

      } else {
        throw new UnsupportedOperationException("Unknown message type: " + msg);
      }

    }

    // If we applied any changes to vertices, we need to call finishAdditions
    // and finishRemovals in the end.
    if (dynamicAdditions) {
      finishAdditions();
    }
    if (dynamicRemovals) {
      finishRemovals();
    }

    return msg;
  }

  private void finishRemovals() throws IOException {
    vertices.finishRemovals();
    // finish the "superstep" because we have written a new file here
    vertices.finishSuperstep();
  }

  private void finishAdditions() throws IOException {
    vertices.finishAdditions();
    // finish the "superstep" because we have written a new file here
    vertices.finishSuperstep();
  }

  public void sendMessage(V vertexID, M msg) throws IOException {
    peer.send(getHostName(vertexID), new GraphJobMessage(vertexID, msg));
  }

  /**
   * @return the destination peer name of the destination of the given directed
   *         edge.
   */
  public String getHostName(V vertexID) {
    return peer.getPeerName(getPartitioner().getPartition(vertexID, null,
        peer.getNumPeers()));
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
    return getAggregationRunner().getLastAggregatedValue(index);
  }

  /**
   * Gets the last aggregated number of vertices at the given index. The index
   * is dependend on how the aggregators were configured during job setup phase.
   * 
   * @return the value of the aggregator, or null if none was defined.
   */
  public final IntWritable getNumLastAggregatedVertices(int index) {
    return getAggregationRunner().getNumLastAggregatedVertices(index);
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

  public int getChangedVertexCnt() {
    return changedVertexCnt;
  }

  public void setChangedVertexCnt(int changedVertexCnt) {
    this.changedVertexCnt = changedVertexCnt;
  }

  /**
   * @return the aggregationRunner
   */
  public AggregationRunner<V, E, M> getAggregationRunner() {
    return aggregationRunner;
  }

  /**
   * @param aggregationRunner the aggregationRunner to set
   */
  void setAggregationRunner(AggregationRunner<V, E, M> aggregationRunner) {
    this.aggregationRunner = aggregationRunner;
  }

}
