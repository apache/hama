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
import java.util.Iterator;
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
 * @param <VERTEX_ID> the id type of a vertex.
 * @param <VERTEX_VALUE> the value type of a vertex.
 * @param <VERTEX_VALUE> the value type of an edge.
 */
public final class GraphJobRunner<VERTEX_ID extends Writable, VERTEX_VALUE extends Writable, EDGE_VALUE_TYPE extends Writable>
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
  private Combiner<VERTEX_VALUE> combiner;

  // multiple aggregator arrays
  private Aggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>[] aggregators;
  private Writable[] globalAggregatorResult;
  private IntWritable[] globalAggregatorIncrement;
  private boolean[] isAbstractAggregator;
  private String[] aggregatorClassNames;
  private Text[] aggregatorValueFlag;
  private Text[] aggregatorIncrementFlag;
  // aggregator on the master side
  private Aggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>[] masterAggregator;

  private Map<VERTEX_ID, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> vertices = new HashMap<VERTEX_ID, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>();

  private String masterTask;
  private boolean updated = true;
  private int globalUpdateCounts = 0;

  private long numberVertices;
  // -1 is deactivated
  private int maxIteration = -1;
  private long iteration;

  // aimed to be accessed by vertex writables to serialize stuff
  Class<VERTEX_ID> vertexIdClass;
  Class<VERTEX_VALUE> vertexValueClass;
  Class<EDGE_VALUE_TYPE> edgeValueClass;
  Class<Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> vertexClass;

  @Override
  @SuppressWarnings("unchecked")
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    this.conf = peer.getConfiguration();
    // Choose one as a master to collect global updates
    this.masterTask = peer.getPeerName(0);

    vertexIdClass = (Class<VERTEX_ID>) conf.getClass(
        GraphJob.VERTEX_ID_CLASS_ATTR, Text.class, Writable.class);
    vertexValueClass = (Class<VERTEX_VALUE>) conf.getClass(
        GraphJob.VERTEX_VALUE_CLASS_ATTR, IntWritable.class, Writable.class);
    edgeValueClass = (Class<EDGE_VALUE_TYPE>) conf.getClass(
        GraphJob.VERTEX_EDGE_VALUE_CLASS_ATTR, IntWritable.class,
        Writable.class);
    vertexClass = (Class<Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) conf
        .getClass("hama.graph.vertex.class", Vertex.class);

    GraphJobMessage.VERTEX_ID_CLASS = vertexIdClass;
    GraphJobMessage.VERTEX_VALUE_CLASS = vertexValueClass;
    GraphJobMessage.VERTEX_CLASS = vertexClass;
    GraphJobMessage.EDGE_VALUE_CLASS = edgeValueClass;

    boolean repairNeeded = conf.getBoolean(GRAPH_REPAIR, false);
    boolean runtimePartitioning = conf.getBoolean(
        GraphJob.VERTEX_GRAPH_RUNTIME_PARTIONING, true);
    Partitioner<VERTEX_ID, VERTEX_VALUE> partitioner = (Partitioner<VERTEX_ID, VERTEX_VALUE>) ReflectionUtils
        .newInstance(
            conf.getClass("bsp.input.partitioner.class", HashPartitioner.class),
            conf);

    if (!conf.getClass(MESSAGE_COMBINER_CLASS, Combiner.class).equals(
        Combiner.class)) {
      LOG.debug("Combiner class: " + conf.get(MESSAGE_COMBINER_CLASS));

      combiner = (Combiner<VERTEX_VALUE>) ReflectionUtils.newInstance(
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

    VertexInputReader<Writable, Writable, VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> reader = (VertexInputReader<Writable, Writable, VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>) ReflectionUtils
        .newInstance(conf.getClass(GraphJob.VERTEX_GRAPH_INPUT_READER,
            VertexInputReader.class), conf);

    loadVertices(peer, repairNeeded, runtimePartitioning, partitioner, reader);
    numberVertices = vertices.size() * peer.getNumPeers();
    // TODO refactor this to a single step
    for (Entry<VERTEX_ID, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> e : vertices
        .entrySet()) {
      LinkedList<VERTEX_VALUE> msgIterator = new LinkedList<VERTEX_VALUE>();
      Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> v = e.getValue();
      msgIterator.add(v.getValue());
      VERTEX_VALUE lastValue = v.getValue();
      v.compute(msgIterator.iterator());
      if (this.aggregators != null) {
        for (int i = 0; i < this.aggregators.length; i++) {
          Aggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> aggregator = this.aggregators[i];
          aggregator.aggregate(v, v.getValue());
          if (isAbstractAggregator[i]) {
            AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> intern = (AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) aggregator;
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
      final Map<VERTEX_ID, LinkedList<VERTEX_VALUE>> messages = parseMessages(peer);
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
            for (int i = 0; i < masterAggregator.length; i++) {
              Writable lastAggregatedValue = masterAggregator[i].getValue();
              if (isAbstractAggregator[i]) {
                final AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> intern = ((AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) aggregators[i]);
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

      int messagesSize = messages.size();
      Iterator<Entry<VERTEX_ID, LinkedList<VERTEX_VALUE>>> iterator = messages
          .entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<VERTEX_ID, LinkedList<VERTEX_VALUE>> e = iterator.next();
        LinkedList<VERTEX_VALUE> msgs = e.getValue();
        if (combiner != null) {
          VERTEX_VALUE combined = combiner.combine(msgs);
          msgs = new LinkedList<VERTEX_VALUE>();
          msgs.add(combined);
        }
        Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> vertex = vertices
            .get(e.getKey());
        VERTEX_VALUE lastValue = vertex.getValue();
        vertex.compute(msgs.iterator());
        if (aggregators != null) {
          if (this.aggregators != null) {
            for (int i = 0; i < this.aggregators.length; i++) {
              Aggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> aggregator = this.aggregators[i];
              aggregator.aggregate(vertex, vertex.getValue());
              if (isAbstractAggregator[i]) {
                AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> intern = ((AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) aggregator);
                intern.aggregate(vertex, lastValue, vertex.getValue());
                intern.aggregateInternal();
              }
            }
          }
        }
        iterator.remove();
      }

      runAggregators(peer, messagesSize);
      iteration++;
    }
  }

  private void runAggregators(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      int messagesSize) throws IOException {
    // send msgCounts to the master task
    MapWritable updatedCnt = new MapWritable();
    updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(messagesSize));
    // also send aggregated values to the master
    if (aggregators != null) {
      for (int i = 0; i < this.aggregators.length; i++) {
        updatedCnt.put(aggregatorValueFlag[i], aggregators[i].getValue());
        if (isAbstractAggregator[i]) {
          updatedCnt
              .put(
                  aggregatorIncrementFlag[i],
                  ((AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) aggregators[i])
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
  private Map<VERTEX_ID, LinkedList<VERTEX_VALUE>> parseMessages(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    GraphJobMessage msg = null;
    final Map<VERTEX_ID, LinkedList<VERTEX_VALUE>> msgMap = new HashMap<VERTEX_ID, LinkedList<VERTEX_VALUE>>();
    while ((msg = peer.getCurrentMessage()) != null) {
      // either this is a vertex message or a directive that must be read
      // as map
      if (msg.isVertexMessage()) {
        final VERTEX_ID vertexID = (VERTEX_ID) msg.getVertexId();
        final VERTEX_VALUE value = (VERTEX_VALUE) msg.getVertexValue();
        LinkedList<VERTEX_VALUE> msgs = msgMap.get(vertexID);
        if (msgs == null) {
          msgs = new LinkedList<VERTEX_VALUE>();
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
            masterAggregator[index]
                .aggregate(null, (VERTEX_VALUE) e.getValue());
          } else if (aggregators != null
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_INCREMENT)) {
            int index = Integer.parseInt(vertexID.toString().split(";")[1]);
            if (isAbstractAggregator[index]) {
              ((AbstractAggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) masterAggregator[index])
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
      boolean repairNeeded,
      boolean runtimePartitioning,
      Partitioner<VERTEX_ID, VERTEX_VALUE> partitioner,
      VertexInputReader<Writable, Writable, VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> reader)
      throws IOException, SyncException, InterruptedException {

    LOG.debug("vertex class: " + vertexClass);
    boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);
    Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> vertex = newVertexInstance(
        vertexClass, conf);
    vertex.peer = peer;
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
        vertex.setEdges(new ArrayList<Edge<VERTEX_ID, EDGE_VALUE_TYPE>>(0));
      }
      if (selfReference) {
        vertex.addEdge(new Edge<VERTEX_ID, EDGE_VALUE_TYPE>(vertex
            .getVertexID(), peer.getPeerName(), null));
      }
      if (runtimePartitioning) {
        int partition = partitioner.getPartition(vertex.getVertexID(),
            vertex.getValue(), peer.getNumPeers());
        // set the destination name for the edge now
        for (Edge<VERTEX_ID, EDGE_VALUE_TYPE> edge : vertex.getEdges()) {
          int edgePartition = partitioner.getPartition(
              edge.getDestinationVertexID(), (VERTEX_VALUE) edge.getValue(),
              peer.getNumPeers());
          edge.destinationPeerName = peer.getPeerName(edgePartition);
        }
        peer.send(peer.getPeerName(partition), new GraphJobMessage(vertex));
      } else {
        vertex.setup(conf);
        vertices.put(vertex.getVertexID(), vertex);
      }
      vertex = newVertexInstance(vertexClass, conf);
      vertex.peer = peer;
      vertex.runner = this;
    }

    if (runtimePartitioning) {
      peer.sync();
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> messagedVertex = (Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>) msg
            .getVertex();
        messagedVertex.peer = peer;
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
      final Collection<Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> entries = vertices
          .values();
      for (Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> entry : entries) {
        List<Edge<VERTEX_ID, EDGE_VALUE_TYPE>> outEdges = entry.getEdges();
        for (Edge<VERTEX_ID, EDGE_VALUE_TYPE> e : outEdges) {
          peer.send(e.getDestinationPeerName(),
              new GraphJobMessage(e.getDestinationVertexID()));
        }
      }
      peer.sync();
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        VERTEX_ID vertexName = (VERTEX_ID) msg.getVertexId();
        if (!vertices.containsKey(vertexName)) {
          Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> newVertex = newVertexInstance(
              vertexClass, conf);
          newVertex.peer = peer;
          newVertex.setVertexID(vertexName);
          newVertex.runner = this;
          if (selfReference) {
            int partition = partitioner.getPartition(newVertex.getVertexID(),
                newVertex.getValue(), peer.getNumPeers());
            String target = peer.getPeerName(partition);
            newVertex.setEdges(Collections
                .singletonList(new Edge<VERTEX_ID, EDGE_VALUE_TYPE>(newVertex
                    .getVertexID(), target, null)));
          } else {
            newVertex.setEdges(new ArrayList<Edge<VERTEX_ID, EDGE_VALUE_TYPE>>(
                0));
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
  public static <VERTEX_ID extends Writable, VERTEX_VALUE extends Writable, EDGE_VALUE_TYPE extends Writable> Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> newVertexInstance(
      Class<?> vertexClass, Configuration conf) {
    @SuppressWarnings("unchecked")
    Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> vertex = (Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>) ReflectionUtils
        .newInstance(vertexClass, conf);
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
    for (Entry<VERTEX_ID, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> e : vertices
        .entrySet()) {
      peer.write(e.getValue().getVertexID(), e.getValue().getValue());
    }
  }

  @SuppressWarnings("unchecked")
  private Aggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> getNewAggregator(
      String clsName) {
    try {
      return (Aggregator<VERTEX_VALUE, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>>) ReflectionUtils
          .newInstance(conf.getClassByName(clsName), conf);
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

  public final Writable getLastAggregatedValue(int index) {
    return globalAggregatorResult[index];
  }

  public final IntWritable getNumLastAggregatedVertices(int index) {
    return globalAggregatorIncrement[index];
  }

}
