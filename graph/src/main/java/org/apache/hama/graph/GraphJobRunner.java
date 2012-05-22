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
    extends
    BSP<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> {

  static final Log LOG = LogFactory.getLog(GraphJobRunner.class);

  // make sure that these values don't collide with the vertex names
  private static final String S_FLAG_MESSAGE_COUNTS = "hama.0";
  private static final String S_FLAG_AGGREGATOR_VALUE = "hama.1";
  private static final String S_FLAG_AGGREGATOR_INCREMENT = "hama.2";

  private static final Text FLAG_MESSAGE_COUNTS = new Text(
      S_FLAG_MESSAGE_COUNTS);
  private static final Text FLAG_AGGREGATOR_VALUE = new Text(
      S_FLAG_AGGREGATOR_VALUE);
  private static final Text FLAG_AGGREGATOR_INCREMENT = new Text(
      S_FLAG_AGGREGATOR_INCREMENT);

  public static final String MESSAGE_COMBINER_CLASS = "hama.vertex.message.combiner.class";
  public static final String GRAPH_REPAIR = "hama.graph.repair";

  private Configuration conf;
  private Combiner<VERTEX_VALUE> combiner;

  private Aggregator<VERTEX_VALUE> aggregator;
  private Writable globalAggregatorResult;
  private IntWritable globalAggregatorIncrement;
  private boolean isAbstractAggregator;

  // aggregator on the master side
  private Aggregator<VERTEX_VALUE> masterAggregator;

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
      BSPPeer<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    this.conf = peer.getConfiguration();
    VertexWritable.CONFIGURATION = conf;
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
        GraphJob.VERTEX_GRAPH_RUNTIME_PARTIONING, false);
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

    if (!conf.getClass("hama.graph.aggregator.class", Aggregator.class).equals(
        Aggregator.class)) {
      LOG.debug("Aggregator class: " + conf.get(MESSAGE_COMBINER_CLASS));

      aggregator = getNewAggregator();
      if (aggregator instanceof AbstractAggregator) {
        isAbstractAggregator = true;
      }
      if (isMasterTask(peer)) {
        masterAggregator = getNewAggregator();
      }
    }

    loadVertices(peer, repairNeeded, runtimePartitioning, partitioner);
    numberVertices = vertices.size() * peer.getNumPeers();
    // TODO refactor this to a single step
    for (Entry<VERTEX_ID, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> e : vertices
        .entrySet()) {
      LinkedList<VERTEX_VALUE> msgIterator = new LinkedList<VERTEX_VALUE>();
      Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> v = e.getValue();
      msgIterator.add(v.getValue());
      VERTEX_VALUE lastValue = v.getValue();
      v.compute(msgIterator.iterator());
      if (aggregator != null) {
        aggregator.aggregate(v.getValue());
        if (isAbstractAggregator) {
          AbstractAggregator<VERTEX_VALUE> intern = ((AbstractAggregator<VERTEX_VALUE>) aggregator);
          intern.aggregate(lastValue, v.getValue());
          intern.aggregateInternal();
        }
      }
    }
    iteration++;
  }

  @Override
  public final void bsp(
      BSPPeer<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer)
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
          if (aggregator != null) {
            Writable lastAggregatedValue = masterAggregator.getValue();
            if (isAbstractAggregator) {
              final AbstractAggregator<VERTEX_VALUE> intern = ((AbstractAggregator<VERTEX_VALUE>) aggregator);
              final Writable finalizeAggregation = intern.finalizeAggregation();
              if (intern.finalizeAggregation() != null) {
                lastAggregatedValue = finalizeAggregation;
              }
              // this count is usually the times of active
              // vertices in the graph
              updatedCnt.put(FLAG_AGGREGATOR_INCREMENT,
                  intern.getTimesAggregated());
            }
            updatedCnt.put(FLAG_AGGREGATOR_VALUE, lastAggregatedValue);
          }
        }
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, new GraphJobMessage(updatedCnt));
        }
      }
      // if we have an aggregator defined, we must make an additional sync
      // to have the updated values available on all our peers.
      if (aggregator != null && iteration > 1) {
        peer.sync();

        MapWritable updatedValues = peer.getCurrentMessage().getMap();
        globalAggregatorResult = updatedValues.get(FLAG_AGGREGATOR_VALUE);
        globalAggregatorIncrement = (IntWritable) updatedValues
            .get(FLAG_AGGREGATOR_INCREMENT);

        aggregator = getNewAggregator();
        if (isMasterTask(peer)) {
          masterAggregator = getNewAggregator();
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
        if (aggregator != null) {
          aggregator.aggregate(vertex.getValue());
          if (isAbstractAggregator) {
            AbstractAggregator<VERTEX_VALUE> intern = ((AbstractAggregator<VERTEX_VALUE>) aggregator);
            intern.aggregate(lastValue, vertex.getValue());
            intern.aggregateInternal();
          }
        }
        iterator.remove();
      }

      // send msgCounts to the master task
      MapWritable updatedCnt = new MapWritable();
      updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(messagesSize));
      // also send aggregated values to the master
      if (aggregator != null) {
        updatedCnt.put(FLAG_AGGREGATOR_VALUE, aggregator.getValue());
        if (isAbstractAggregator) {
          updatedCnt.put(FLAG_AGGREGATOR_INCREMENT,
              ((AbstractAggregator<VERTEX_VALUE>) aggregator)
                  .getTimesAggregated());
        }
      }
      peer.send(masterTask, new GraphJobMessage(updatedCnt));
      iteration++;
    }
  }

  @SuppressWarnings("unchecked")
  private Map<VERTEX_ID, LinkedList<VERTEX_VALUE>> parseMessages(
      BSPPeer<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer)
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
          VERTEX_ID vertexID = (VERTEX_ID) e.getKey();
          if (FLAG_MESSAGE_COUNTS.equals(vertexID)) {
            if (((IntWritable) e.getValue()).get() == Integer.MIN_VALUE) {
              updated = false;
            } else {
              globalUpdateCounts += ((IntWritable) e.getValue()).get();
            }
          } else if (aggregator != null
              && FLAG_AGGREGATOR_VALUE.equals(vertexID)) {
            masterAggregator.aggregate((VERTEX_VALUE) e.getValue());
          } else if (aggregator != null
              && FLAG_AGGREGATOR_INCREMENT.equals(vertexID)) {
            if (isAbstractAggregator) {
              ((AbstractAggregator<VERTEX_VALUE>) masterAggregator)
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
      BSPPeer<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer,
      boolean repairNeeded, boolean runtimePartitioning,
      Partitioner<VERTEX_ID, VERTEX_VALUE> partitioner) throws IOException,
      SyncException, InterruptedException {
    LOG.debug("vertex class: " + vertexClass);
    boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);
    KeyValuePair<? extends VertexWritable<VERTEX_ID, VERTEX_VALUE>, ? extends VertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> vertex = newVertexInstance(
          vertexClass, conf);
      vertex.setVertexID(next.getKey().getVertexId());
      vertex.peer = peer;
      vertex.runner = this;

      VertexWritable<VERTEX_ID, VERTEX_VALUE>[] arr = (VertexWritable[]) next
          .getValue().toArray();
      if (selfReference) {
        VertexWritable<VERTEX_ID, VERTEX_VALUE>[] tmp = new VertexWritable[arr.length + 1];
        System.arraycopy(arr, 0, tmp, 0, arr.length);
        tmp[arr.length] = new VertexWritable<VERTEX_ID, VERTEX_VALUE>(
            vertex.getValue(), vertex.getVertexID(), vertexIdClass,
            vertexValueClass);
        arr = tmp;
      }
      List<Edge<VERTEX_ID, EDGE_VALUE_TYPE>> edges = new ArrayList<Edge<VERTEX_ID, EDGE_VALUE_TYPE>>();
      for (VertexWritable<VERTEX_ID, VERTEX_VALUE> e : arr) {
        int partition = partitioner.getPartition(e.getVertexId(),
            e.getVertexValue(), peer.getNumPeers());
        String target = peer.getPeerName(partition);
        edges.add(new Edge<VERTEX_ID, EDGE_VALUE_TYPE>(e.getVertexId(), target,
            (EDGE_VALUE_TYPE) e.getVertexValue()));
      }

      vertex.edges = edges;
      if (runtimePartitioning) {
        int partition = partitioner.getPartition(vertex.getVertexID(),
            vertex.getValue(), peer.getNumPeers());
        peer.send(peer.getPeerName(partition), new GraphJobMessage(vertex));
      } else {
        vertex.setup(conf);
        vertices.put(next.getKey().getVertexId(), vertex);
      }
    }

    if (runtimePartitioning) {
      peer.sync();
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> vertex = (Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>) msg
            .getVertex();
        vertex.peer = peer;
        vertex.runner = this;
        vertex.setup(conf);
        vertices.put(vertex.getVertexID(), vertex);
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
        List<Edge<VERTEX_ID, EDGE_VALUE_TYPE>> outEdges = entry.getOutEdges();
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
          Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE> vertex = newVertexInstance(
              vertexClass, conf);
          vertex.peer = peer;
          vertex.setVertexID(vertexName);
          vertex.runner = this;
          if (selfReference) {
            int partition = partitioner.getPartition(vertex.getVertexID(),
                vertex.getValue(), peer.getNumPeers());
            String target = peer.getPeerName(partition);
            vertex.edges = Collections
                .singletonList(new Edge<VERTEX_ID, EDGE_VALUE_TYPE>(vertex
                    .getVertexID(), target, null));
          } else {
            vertex.edges = Collections.emptyList();
          }
          vertex.setup(conf);
          vertices.put(vertexName, vertex);
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
      BSPPeer<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    for (Entry<VERTEX_ID, Vertex<VERTEX_ID, VERTEX_VALUE, EDGE_VALUE_TYPE>> e : vertices
        .entrySet()) {
      peer.write(e.getValue().getVertexID(), e.getValue().getValue());
    }
  }

  @SuppressWarnings("unchecked")
  private Aggregator<VERTEX_VALUE> getNewAggregator() {
    return (Aggregator<VERTEX_VALUE>) ReflectionUtils.newInstance(
        conf.getClass("hama.graph.aggregator.class", Aggregator.class), conf);
  }

  private boolean isMasterTask(
      BSPPeer<VertexWritable<VERTEX_ID, VERTEX_VALUE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer) {
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

  public final Writable getLastAggregatedValue() {
    return globalAggregatorResult;
  }

  public final IntWritable getNumLastAggregatedVertices() {
    return globalAggregatorIncrement;
  }

}
