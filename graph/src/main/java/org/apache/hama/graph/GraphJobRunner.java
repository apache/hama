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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

/**
 * Fully generic graph job runner.
 * 
 * @param <V> the id type of a vertex.
 * @param <E> the value type of an edge.
 * @param <M> the value type of a vertex.
 */
public final class GraphJobRunner<V extends Writable, E extends Writable, M extends Writable>
    extends BSP<Writable, Writable, Writable, Writable, GraphJobMessage> {

  static final Log LOG = LogFactory.getLog(GraphJobRunner.class);

  // make sure that these values don't collide with the vertex names
  private static final String S_FLAG_MESSAGE_COUNTS = "hama.0";
  private static final String S_FLAG_AGGREGATOR_VALUE = "hama.1";
  private static final String S_FLAG_AGGREGATOR_INCREMENT = "hama.2";

  private static final Text FLAG_MESSAGE_COUNTS = new Text(
      S_FLAG_MESSAGE_COUNTS);

  public static final String MESSAGE_COMBINER_CLASS = "hama.vertex.message.combiner.class";
  public static final String GRAPH_REPAIR = "hama.graph.repair";

  private Configuration conf;
  private Combiner<M> combiner;
  private Partitioner<V, M> partitioner;

  // multiple aggregator arrays
  private Aggregator<M, Vertex<V, E, M>>[] aggregators;
  private Writable[] globalAggregatorResult;
  private IntWritable[] globalAggregatorIncrement;
  private boolean[] isAbstractAggregator;
  private String[] aggregatorClassNames;
  private Text[] aggregatorValueFlag;
  private Text[] aggregatorIncrementFlag;
  // aggregator on the master side
  private Aggregator<M, Vertex<V, E, M>>[] masterAggregator;

  private Map<V, Vertex<V, E, M>> vertices = new HashMap<V, Vertex<V, E, M>>();

  private String masterTask;
  private boolean updated = true;
  private int globalUpdateCounts = 0;

  private long numberVertices;
  // -1 is deactivated
  private int maxIteration = -1;
  private long iteration;

  private Class<V> vertexIdClass;
  private Class<M> vertexValueClass;
  private Class<E> edgeValueClass;
  private Class<Vertex<V, E, M>> vertexClass;

  @Override
  @SuppressWarnings("unchecked")
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    this.conf = peer.getConfiguration();
    // Choose one as a master to collect global updates
    this.masterTask = peer.getPeerName(0);

    vertexIdClass = (Class<V>) conf.getClass(GraphJob.VERTEX_ID_CLASS_ATTR,
        Text.class, Writable.class);
    vertexValueClass = (Class<M>) conf.getClass(
        GraphJob.VERTEX_VALUE_CLASS_ATTR, IntWritable.class, Writable.class);
    edgeValueClass = (Class<E>) conf.getClass(
        GraphJob.VERTEX_EDGE_VALUE_CLASS_ATTR, IntWritable.class,
        Writable.class);
    vertexClass = (Class<Vertex<V, E, M>>) conf.getClass(
        "hama.graph.vertex.class", Vertex.class);

    GraphJobMessage.VERTEX_ID_CLASS = vertexIdClass;
    GraphJobMessage.VERTEX_VALUE_CLASS = vertexValueClass;
    GraphJobMessage.VERTEX_CLASS = vertexClass;
    GraphJobMessage.EDGE_VALUE_CLASS = edgeValueClass;

    boolean repairNeeded = conf.getBoolean(GRAPH_REPAIR, false);
    boolean runtimePartitioning = conf.getBoolean(
        GraphJob.VERTEX_GRAPH_RUNTIME_PARTIONING, true);
    partitioner = (Partitioner<V, M>) ReflectionUtils.newInstance(
        conf.getClass("bsp.input.partitioner.class", HashPartitioner.class),
        conf);

    if (!conf.getClass(MESSAGE_COMBINER_CLASS, Combiner.class).equals(
        Combiner.class)) {
      LOG.debug("Combiner class: " + conf.get(MESSAGE_COMBINER_CLASS));

      combiner = (Combiner<M>) ReflectionUtils.newInstance(
          conf.getClass("hama.vertex.message.combiner.class", Combiner.class),
          conf);
    }
    String aggregatorClasses = conf.get(GraphJob.AGGREGATOR_CLASS_ATTR);
    if (aggregatorClasses != null) {
      LOG.debug("Aggregator classes: " + aggregatorClasses);
      aggregatorClassNames = aggregatorClasses.split(";");
      // init to the split size
      aggregators = new Aggregator[aggregatorClassNames.length];
      globalAggregatorResult = new Writable[aggregatorClassNames.length];
      globalAggregatorIncrement = new IntWritable[aggregatorClassNames.length];
      isAbstractAggregator = new boolean[aggregatorClassNames.length];
      aggregatorValueFlag = new Text[aggregatorClassNames.length];
      aggregatorIncrementFlag = new Text[aggregatorClassNames.length];
      if (isMasterTask(peer)) {
        masterAggregator = new Aggregator[aggregatorClassNames.length];
      }
      for (int i = 0; i < aggregatorClassNames.length; i++) {
        aggregators[i] = getNewAggregator(aggregatorClassNames[i]);
        aggregatorValueFlag[i] = new Text(S_FLAG_AGGREGATOR_VALUE + ";" + i);
        aggregatorIncrementFlag[i] = new Text(S_FLAG_AGGREGATOR_INCREMENT + ";"
            + i);
        if (aggregators[i] instanceof AbstractAggregator) {
          isAbstractAggregator[i] = true;
        }
        if (isMasterTask(peer)) {
          masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
        }
      }
    }

    VertexInputReader<Writable, Writable, V, E, M> reader = (VertexInputReader<Writable, Writable, V, E, M>) ReflectionUtils
        .newInstance(conf.getClass(GraphJob.VERTEX_GRAPH_INPUT_READER,
            VertexInputReader.class), conf);

    loadVertices(peer, repairNeeded, runtimePartitioning, partitioner, reader);
    numberVertices = vertices.size() * peer.getNumPeers();
    // TODO refactor this to a single step
    for (Entry<V, Vertex<V, E, M>> e : vertices.entrySet()) {
      LinkedList<M> msgIterator = new LinkedList<M>();
      Vertex<V, E, M> v = e.getValue();
      msgIterator.add(v.getValue());
      M lastValue = v.getValue();
      v.compute(msgIterator.iterator());
      if (this.aggregators != null) {
        for (int i = 0; i < this.aggregators.length; i++) {
          Aggregator<M, Vertex<V, E, M>> aggregator = this.aggregators[i];
          aggregator.aggregate(v, v.getValue());
          if (isAbstractAggregator[i]) {
            AbstractAggregator<M, Vertex<V, E, M>> intern = (AbstractAggregator<M, Vertex<V, E, M>>) aggregator;
            intern.aggregate(v, lastValue, v.getValue());
            intern.aggregateInternal();
          }
        }
      }
    }
    runAggregators(peer, 1);
    iteration++;
  }

  @Override
  public final void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {

    maxIteration = peer.getConfiguration().getInt("hama.graph.max.iteration",
        -1);

    while (updated && !((maxIteration > 0) && iteration > maxIteration)) {
      globalUpdateCounts = 0;
      peer.sync();

      // Map <vertexID, messages>
      final Map<V, LinkedList<M>> messages = parseMessages(peer);
      // use iterations here, since repair can skew the number of
      // supersteps
      if (isMasterTask(peer) && iteration > 1) {
        MapWritable updatedCnt = new MapWritable();
        // exit if there's no update made
        if (globalUpdateCounts == 0) {
          updatedCnt.put(FLAG_MESSAGE_COUNTS,
              new IntWritable(Integer.MIN_VALUE));
        } else {
          if (aggregators != null) {
            // work through the master aggregators
            for (int i = 0; i < masterAggregator.length; i++) {
              Writable lastAggregatedValue = masterAggregator[i].getValue();
              if (isAbstractAggregator[i]) {
                final AbstractAggregator<M, Vertex<V, E, M>> intern = ((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[i]);
                final Writable finalizeAggregation = intern
                    .finalizeAggregation();
                if (intern.finalizeAggregation() != null) {
                  lastAggregatedValue = finalizeAggregation;
                }
                // this count is usually the times of active
                // vertices in the graph
                updatedCnt.put(aggregatorIncrementFlag[i],
                    intern.getTimesAggregated());
              }
              updatedCnt.put(aggregatorValueFlag[i], lastAggregatedValue);
            }
          }
        }
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, new GraphJobMessage(updatedCnt));
        }
      }
      // if we have an aggregator defined, we must make an additional sync
      // to have the updated values available on all our peers.
      if (aggregators != null && iteration > 1) {
        peer.sync();

        MapWritable updatedValues = peer.getCurrentMessage().getMap();
        for (int i = 0; i < aggregators.length; i++) {
          globalAggregatorResult[i] = updatedValues.get(aggregatorValueFlag[i]);
          globalAggregatorIncrement[i] = (IntWritable) updatedValues
              .get(aggregatorIncrementFlag[i]);
        }
        IntWritable count = (IntWritable) updatedValues
            .get(FLAG_MESSAGE_COUNTS);
        if (count != null && count.get() == Integer.MIN_VALUE) {
          updated = false;
          break;
        }
      }

      int activeVertices = 0;
      for (Vertex<V, E, M> vertex : vertices.values()) {
        LinkedList<M> msgs = messages.get(vertex.getVertexID());
        // If there are newly received messages, restart.
        if (vertex.isHalted() && msgs != null) {
          vertex.setActive();
        }
        if (msgs == null) {
          msgs = new LinkedList<M>();
        }

        if (!vertex.isHalted()) {
          if (combiner != null) {
            M combined = combiner.combine(msgs);
            msgs = new LinkedList<M>();
            msgs.add(combined);
          }
          M lastValue = vertex.getValue();
          vertex.compute(msgs.iterator());

          if (aggregators != null) {
            if (this.aggregators != null) {
              for (int i = 0; i < this.aggregators.length; i++) {
                Aggregator<M, Vertex<V, E, M>> aggregator = this.aggregators[i];
                aggregator.aggregate(vertex, vertex.getValue());
                if (isAbstractAggregator[i]) {
                  AbstractAggregator<M, Vertex<V, E, M>> intern = ((AbstractAggregator<M, Vertex<V, E, M>>) aggregator);
                  intern.aggregate(vertex, lastValue, vertex.getValue());
                  intern.aggregateInternal();
                }
              }
            }
          }
          if (!vertex.isHalted()) {
            activeVertices++;
          }
        }
      }

      runAggregators(peer, activeVertices);
      iteration++;
    }
  }

  private void runAggregators(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      int activeVertices) throws IOException {
    // send msgCounts to the master task
    MapWritable updatedCnt = new MapWritable();
    updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(activeVertices));
    // also send aggregated values to the master
    if (aggregators != null) {
      for (int i = 0; i < this.aggregators.length; i++) {
        updatedCnt.put(aggregatorValueFlag[i], aggregators[i].getValue());
        if (isAbstractAggregator[i]) {
          updatedCnt.put(aggregatorIncrementFlag[i],
              ((AbstractAggregator<M, Vertex<V, E, M>>) aggregators[i])
                  .getTimesAggregated());
        }
      }
      for (int i = 0; i < aggregators.length; i++) {
        // now create new aggregators for the next iteration
        aggregators[i] = getNewAggregator(aggregatorClassNames[i]);
        if (isMasterTask(peer)) {
          masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
        }
      }
    }
    peer.send(masterTask, new GraphJobMessage(updatedCnt));
  }

  @SuppressWarnings("unchecked")
  private Map<V, LinkedList<M>> parseMessages(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    GraphJobMessage msg = null;
    final Map<V, LinkedList<M>> msgMap = new HashMap<V, LinkedList<M>>();
    while ((msg = peer.getCurrentMessage()) != null) {
      // either this is a vertex message or a directive that must be read
      // as map
      if (msg.isVertexMessage()) {
        final V vertexID = (V) msg.getVertexId();
        final M value = (M) msg.getVertexValue();
        LinkedList<M> msgs = msgMap.get(vertexID);
        if (msgs == null) {
          msgs = new LinkedList<M>();
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
          } else if (aggregators != null
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_VALUE)) {
            int index = Integer.parseInt(vertexID.toString().split(";")[1]);
            masterAggregator[index].aggregate(null, (M) e.getValue());
          } else if (aggregators != null
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_INCREMENT)) {
            int index = Integer.parseInt(vertexID.toString().split(";")[1]);
            if (isAbstractAggregator[index]) {
              ((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[index])
                  .addTimesAggregated(((IntWritable) e.getValue()).get());
            }
          }
        }
      } else {
        throw new UnsupportedOperationException("Unknown message type? " + msg);
      }

    }
    return msgMap;
  }

  @SuppressWarnings("unchecked")
  private void loadVertices(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      boolean repairNeeded, boolean runtimePartitioning,
      Partitioner<V, M> partitioner,
      VertexInputReader<Writable, Writable, V, E, M> reader)
      throws IOException, SyncException, InterruptedException {

    LOG.debug("vertex class: " + vertexClass);
    boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);
    Vertex<V, E, M> vertex = newVertexInstance(vertexClass, conf);
    vertex.setPeer(peer);
    vertex.runner = this;
    while (true) {
      KeyValuePair<Writable, Writable> next = peer.readNext();
      if (next == null) {
        break;
      }
      boolean vertexFinished = reader.parseVertex(next.getKey(),
          next.getValue(), vertex);
      if (!vertexFinished) {
        continue;
      }
      if (vertex.getEdges() == null) {
        vertex.setEdges(new ArrayList<Edge<V, E>>(0));
      }
      if (selfReference) {
        vertex.addEdge(new Edge<V, E>(vertex.getVertexID(), peer.getPeerName(),
            null));
      }
      if (runtimePartitioning) {
        int partition = partitioner.getPartition(vertex.getVertexID(),
            vertex.getValue(), peer.getNumPeers());
        // set the destination name for the edge now
        for (Edge<V, E> edge : vertex.getEdges()) {
          int edgePartition = partitioner.getPartition(
              edge.getDestinationVertexID(), (M) edge.getValue(),
              peer.getNumPeers());
          edge.destinationPeerName = peer.getPeerName(edgePartition);
        }
        peer.send(peer.getPeerName(partition), new GraphJobMessage(vertex));
      } else {
        vertex.setup(conf);
        vertices.put(vertex.getVertexID(), vertex);
      }
      vertex = newVertexInstance(vertexClass, conf);
      vertex.setPeer(peer);
      vertex.runner = this;
    }

    if (runtimePartitioning) {
      peer.sync();
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        Vertex<V, E, M> messagedVertex = (Vertex<V, E, M>) msg.getVertex();
        messagedVertex.setPeer(peer);
        messagedVertex.runner = this;
        messagedVertex.setup(conf);
        vertices.put(messagedVertex.getVertexID(), messagedVertex);
      }
    }

    /*
     * If the user want to repair the graph, it should traverse through that
     * local chunk of adjancency list and message the corresponding peer to
     * check whether that vertex exists. In real-life this may be dead-ending
     * vertices, since we have no information about outgoing edges. Mainly this
     * procedure is to prevent NullPointerExceptions from happening.
     */
    if (repairNeeded) {
      LOG.debug("Starting repair of this graph!");
      final Collection<Vertex<V, E, M>> entries = vertices.values();
      for (Vertex<V, E, M> entry : entries) {
        List<Edge<V, E>> outEdges = entry.getEdges();
        for (Edge<V, E> e : outEdges) {
          peer.send(e.getDestinationPeerName(),
              new GraphJobMessage(e.getDestinationVertexID()));
        }
      }
      peer.sync();
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        V vertexName = (V) msg.getVertexId();
        if (!vertices.containsKey(vertexName)) {
          Vertex<V, E, M> newVertex = newVertexInstance(vertexClass, conf);
          newVertex.setPeer(peer);
          newVertex.setVertexID(vertexName);
          newVertex.runner = this;
          if (selfReference) {
            int partition = partitioner.getPartition(newVertex.getVertexID(),
                newVertex.getValue(), peer.getNumPeers());
            String target = peer.getPeerName(partition);
            newVertex.setEdges(Collections.singletonList(new Edge<V, E>(
                newVertex.getVertexID(), target, null)));
          } else {
            newVertex.setEdges(new ArrayList<Edge<V, E>>(0));
          }
          newVertex.setup(conf);
          vertices.put(vertexName, newVertex);
        }
      }
    }

  }

  /**
   * @return a new vertex instance
   */
  public static <V extends Writable, E extends Writable, M extends Writable> Vertex<V, E, M> newVertexInstance(
      Class<?> vertexClass, Configuration conf) {
    @SuppressWarnings("unchecked")
    Vertex<V, E, M> vertex = (Vertex<V, E, M>) ReflectionUtils.newInstance(
        vertexClass, conf);
    return vertex;
  }

  /**
   * Just write <ID as Writable, Value as Writable> pair as a result. Note that
   * this will also be executed when failure happened.
   */
  @Override
  public final void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    for (Entry<V, Vertex<V, E, M>> e : vertices.entrySet()) {
      peer.write(e.getValue().getVertexID(), e.getValue().getValue());
    }
  }

  @SuppressWarnings("unchecked")
  private Aggregator<M, Vertex<V, E, M>> getNewAggregator(String clsName) {
    try {
      return (Aggregator<M, Vertex<V, E, M>>) ReflectionUtils.newInstance(
          conf.getClassByName(clsName), conf);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    throw new IllegalArgumentException("Aggregator class " + clsName
        + " could not be found or instantiated!");
  }

  private boolean isMasterTask(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    return peer.getPeerName().equals(masterTask);
  }

  public final long getNumberVertices() {
    return numberVertices;
  }

  public final long getNumberIterations() {
    return iteration;
  }

  public final int getMaxIteration() {
    return maxIteration;
  }

  public Partitioner<V, M> getPartitioner() {
    return partitioner;
  }

  public final Writable getLastAggregatedValue(int index) {
    return globalAggregatorResult[index];
  }

  public final IntWritable getNumLastAggregatedVertices(int index) {
    return globalAggregatorIncrement[index];
  }

}
