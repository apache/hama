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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.util.ReflectionUtils;
import org.apache.hama.util.UnsafeByteArrayInputStream;
import org.apache.hama.util.WritableUtils;

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
  public static final String DEFAULT_THREAD_POOL_SIZE = "hama.graph.thread.pool.size";

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

  // global counter for thread exceptions
  // TODO find more graceful way to handle thread exceptions.
  private AtomicInteger errorCount = new AtomicInteger(0);

  private AggregationRunner<V, E, M> aggregationRunner;
  private VertexOutputWriter<Writable, Writable, V, E, M> vertexOutputWriter;
  private Combiner<Writable> combiner;

  private BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer;

  private RejectedExecutionHandler retryHandler = new RetryRejectedExecutionHandler();

  // Below maps are used for grouping messages into single GraphJobMessage,
  // based on vertex ID.
  private final ConcurrentHashMap<Integer, GraphJobMessage> partitionMessages = new ConcurrentHashMap<Integer, GraphJobMessage>();
  private final ConcurrentHashMap<V, GraphJobMessage> vertexMessages = new ConcurrentHashMap<V, GraphJobMessage>();

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

    if (peer.getSuperstepCount() == 2) {
      startTime = System.currentTimeMillis();
      doInitialSuperstep(peer);
      LOG.info("Total time spent for initial superstep: "
          + (System.currentTimeMillis() - startTime) + " ms");
    }
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
      doSuperstep(firstVertexMessage, peer);
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

    final String combinerName = conf.get(Constants.COMBINER_CLASS);
    if (combinerName != null) {
      try {
        combiner = (Combiner<Writable>) ReflectionUtils
            .newInstance(combinerName);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
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
    this.errorCount.set(0);
    long startTime = System.currentTimeMillis();

    this.changedVertexCnt = 0;
    vertices.startSuperstep();

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(conf.getInt(DEFAULT_THREAD_POOL_SIZE, 64));
    executor.setRejectedExecutionHandler(retryHandler);

    long loopStartTime = System.currentTimeMillis();
    while (currentMessage != null) {
      executor.execute(new ComputeRunnable(currentMessage));

      currentMessage = peer.getCurrentMessage();
    }
    LOG.info("Total time spent for superstep-" + peer.getSuperstepCount()
        + " looping: " + (System.currentTimeMillis() - loopStartTime) + " ms");

    executor.shutdown();
    try {
      executor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    if (errorCount.get() > 0) {
      throw new IOException("there were " + errorCount
          + " exceptions during compute vertices.");
    }

    Iterator it = vertices.iterator();
    while (it.hasNext()) {
      Vertex<V, E, M> vertex = (Vertex<V, E, M>) it.next();
      if (!vertex.isHalted() && !vertex.isComputed()) {
        vertex.compute(Collections.<M> emptyList());
        vertices.finishVertexComputation(vertex);
      }
    }

    getAggregationRunner().sendAggregatorValues(peer,
        vertices.getActiveVerticesNum(), this.changedVertexCnt);
    this.iteration++;

    LOG.info("Total time spent for superstep-" + peer.getSuperstepCount()
        + " computing vertices: " + (System.currentTimeMillis() - startTime)
        + " ms");

    startTime = System.currentTimeMillis();
    finishSuperstep();
    LOG.info("Total time spent for superstep-" + peer.getSuperstepCount()
        + " synchronizing: " + (System.currentTimeMillis() - startTime) + " ms");
  }

  /**
   * Seed the vertices first with their own values in compute. This is the first
   * superstep after the vertices have been loaded.
   */
  private void doInitialSuperstep(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    this.changedVertexCnt = 0;
    this.errorCount.set(0);
    vertices.startSuperstep();

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(conf.getInt(DEFAULT_THREAD_POOL_SIZE, 64));
    executor.setRejectedExecutionHandler(retryHandler);

    for (V v : vertices.keySet()) {
      executor.execute(new ComputeRunnable(v));
    }

    executor.shutdown();
    try {
      executor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    if (errorCount.get() > 0) {
      throw new IOException("there were " + errorCount
          + " exceptions during compute vertices.");
    }

    getAggregationRunner().sendAggregatorValues(peer, 1, this.changedVertexCnt);
    iteration++;
    finishSuperstep();
  }

  public void incrementErrorCount() {
    errorCount.incrementAndGet();
  }

  class ComputeRunnable implements Runnable {
    Vertex<V, E, M> vertex;
    Iterable<M> msgs;

    @SuppressWarnings("unchecked")
    public ComputeRunnable(GraphJobMessage msg) throws IOException {
      this.vertex = vertices.get((V) msg.getVertexId());
      this.msgs = (Iterable<M>) getIterableMessages(msg.getValuesBytes(),
          msg.getNumOfValues());
    }

    public ComputeRunnable(V v) throws IOException {
      this.vertex = vertices.get(v);
    }

    @Override
    public void run() {
      try {
        // call once at initial superstep
        if (iteration == 0) {
          vertex.setup(conf);
          msgs = Collections.singleton(vertex.getValue());
        }

        vertex.compute(msgs);
        vertices.finishVertexComputation(vertex);
      } catch (IOException e) {
        incrementErrorCount();
        throw new RuntimeException(e);
      }
    }
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

    for (int i = 0; i < peer.getNumPeers(); i++) {
      partitionMessages.put(i, new GraphJobMessage());
    }

    VertexInputReader<Writable, Writable, V, E, M> reader = (VertexInputReader<Writable, Writable, V, E, M>) ReflectionUtils
        .newInstance(conf.getClass(Constants.RUNTIME_PARTITION_RECORDCONVERTER,
            VertexInputReader.class));

    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors
        .newCachedThreadPool();
    executor.setMaximumPoolSize(conf.getInt(DEFAULT_THREAD_POOL_SIZE, 64));
    executor.setRejectedExecutionHandler(retryHandler);

    KeyValuePair<Writable, Writable> next = null;

    while ((next = peer.readNext()) != null) {
      Vertex<V, E, M> vertex = GraphJobRunner
          .<V, E, M> newVertexInstance(VERTEX_CLASS);

      boolean vertexFinished = false;
      try {
        vertexFinished = reader.parseVertex(next.getKey(), next.getValue(),
            vertex);
      } catch (Exception e) {
        throw new IOException("Parse exception occured: " + e);
      }

      if (!vertexFinished) {
        continue;
      }

      Runnable worker = new Parser(vertex);
      executor.execute(worker);

    }

    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    Iterator<Entry<Integer, GraphJobMessage>> it;
    it = partitionMessages.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Integer, GraphJobMessage> e = it.next();
      it.remove();
      GraphJobMessage msg = e.getValue();
      msg.setFlag(GraphJobMessage.PARTITION_FLAG);
      peer.send(getHostName(e.getKey()), msg);
    }

    peer.sync();

    executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    executor.setMaximumPoolSize(conf.getInt(DEFAULT_THREAD_POOL_SIZE, 64));
    executor.setRejectedExecutionHandler(retryHandler);

    GraphJobMessage msg;
    while ((msg = peer.getCurrentMessage()) != null) {
      executor.execute(new AddVertex(msg));
    }

    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    LOG.info(vertices.size() + " vertices are loaded into "
        + peer.getPeerName());
  }

  class AddVertex implements Runnable {
    GraphJobMessage msg;

    public AddVertex(GraphJobMessage msg) {
      this.msg = msg;
    }

    @Override
    public void run() {
      ByteArrayInputStream bis = new ByteArrayInputStream(msg.getValuesBytes());
      DataInputStream dis = new DataInputStream(bis);

      for (int i = 0; i < msg.getNumOfValues(); i++) {
        try {
          Vertex<V, E, M> vertex = newVertexInstance(VERTEX_CLASS);
          vertex.readFields(dis);

          addVertex(vertex);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

  }

  class Parser implements Runnable {
    Vertex<V, E, M> vertex;

    public Parser(Vertex<V, E, M> vertex) {
      this.vertex = vertex;
    }

    @Override
    public void run() {
      try {
        int partition = getPartitionID(vertex.getVertexID());

        if (peer.getPeerIndex() == partition) {
          addVertex(vertex);
        } else {
          partitionMessages.get(partition).add(WritableUtils.serialize(vertex));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
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
  }

  private void finishAdditions() throws IOException {
    vertices.finishAdditions();
  }

  public void sendMessage(V vertexID, M msg) throws IOException {
    if (!vertexMessages.containsKey(vertexID)) {
      vertexMessages.putIfAbsent(vertexID, new GraphJobMessage());
    }
    if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
      vertexMessages.get(vertexID).add(WritableUtils.serialize(msg));
    } else {
      vertexMessages.get(vertexID).add(WritableUtils.unsafeSerialize(msg));
    }
  }

  public void sendMessage(List<Edge<V, E>> outEdges, M msg) throws IOException {
    byte[] serialized;
    if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
      serialized = WritableUtils.serialize(msg);
    } else {
      serialized = WritableUtils.unsafeSerialize(msg);
    }

    for (Edge<V, E> e : outEdges) {
      if (!vertexMessages.containsKey(e.getDestinationVertexID())) {
        vertexMessages.putIfAbsent(e.getDestinationVertexID(),
            new GraphJobMessage());
      }

      vertexMessages.get(e.getDestinationVertexID()).add(serialized);
    }
  }

  public void finishSuperstep() throws IOException {
    vertices.finishSuperstep();

    Iterator<Entry<V, GraphJobMessage>> it = vertexMessages.entrySet()
        .iterator();
    while (it.hasNext()) {
      Entry<V, GraphJobMessage> e = it.next();
      it.remove();

      if (combiner != null && e.getValue().getNumOfValues() > 1) {
        GraphJobMessage combined;
        combined = new GraphJobMessage(e.getKey(),
            WritableUtils.serialize(combiner.combine(getIterableMessages(e
                .getValue().getValuesBytes(), e.getValue().getNumOfValues()))));

        combined.setFlag(GraphJobMessage.VERTEX_FLAG);
        peer.send(getHostName(e.getKey()), combined);
      } else {
        // set vertexID
        e.getValue().setVertexId(e.getKey());
        e.getValue().setFlag(GraphJobMessage.VERTEX_FLAG);
        peer.send(getHostName(e.getKey()), e.getValue());
      }
    }

    if (isMasterTask(peer)) {
      peer.getCounter(GraphJobCounter.ITERATIONS).increment(1);
    }
  }

  public Iterable<Writable> getIterableMessages(final byte[] valuesBytes,
      final int numOfValues) {

    return new Iterable<Writable>() {
      DataInputStream dis;

      @Override
      public Iterator<Writable> iterator() {
        if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
          dis = new DataInputStream(new ByteArrayInputStream(valuesBytes));
        } else {
          dis = new DataInputStream(new UnsafeByteArrayInputStream(valuesBytes));
        }

        return new Iterator<Writable>() {
          int index = 0;

          @Override
          public boolean hasNext() {
            return (index < numOfValues) ? true : false;
          }

          @Override
          public Writable next() {
            Writable v = createVertexValue();
            try {
              v.readFields(dis);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            index++;
            return v;
          }

          @Override
          public void remove() {
          }
        };
      }
    };
  }

  class RetryRejectedExecutionHandler implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.error(e);
      }
      executor.execute(r);
    }

  }

  /**
   * @return the destination peer name of the destination of the given directed
   *         edge.
   */
  public String getHostName(V vertexID) {
    return peer.getPeerName(partitioner.getPartition(vertexID, null,
        peer.getNumPeers()));
  }

  public int getPartitionID(V vertexID) {
    return partitioner.getPartition(vertexID, null, peer.getNumPeers());
  }

  public String getHostName(int partitionID) {
    return peer.getPeerName(partitionID);
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
