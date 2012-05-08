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
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class GraphJobRunner extends BSP {

  public static final Log LOG = LogFactory.getLog(GraphJobRunner.class);

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

  private static final String MESSAGE_COMBINER_CLASS = "hama.vertex.message.combiner.class";

  private Configuration conf;
  private Combiner<? extends Writable> combiner;

  private Aggregator<Writable> aggregator;
  private Writable globalAggregatorResult;
  private IntWritable globalAggregatorIncrement;
  private boolean isAbstractAggregator;

  // aggregator on the master side
  private Aggregator<Writable> masterAggregator;

  private Map<String, Vertex> vertices = new HashMap<String, Vertex>();

  private String masterTask;
  private boolean updated = true;
  private int globalUpdateCounts = 0;

  private long numberVertices;
  // -1 is deactivated
  private int maxIteration = -1;
  private long iteration;

  // TODO check if our graph is not broken and repair
  public void setup(BSPPeer peer) throws IOException, SyncException,
      InterruptedException {
    this.conf = peer.getConfiguration();
    // Choose one as a master to collect global updates
    this.masterTask = peer.getPeerName(0);

    if (!conf.getClass(MESSAGE_COMBINER_CLASS, Combiner.class).equals(
        Combiner.class)) {
      LOG.debug("Combiner class: " + conf.get(MESSAGE_COMBINER_CLASS));

      combiner = (Combiner<? extends Writable>) ReflectionUtils.newInstance(
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

    loadVertices(peer);
    numberVertices = vertices.size() * peer.getNumPeers();
    // TODO refactor this to a single step
    for (Map.Entry<String, Vertex> e : vertices.entrySet()) {
      LinkedList<Writable> msgIterator = new LinkedList<Writable>();
      Vertex v = e.getValue();
      msgIterator.add(v.getValue());
      Writable lastValue = v.getValue();
      v.compute(msgIterator.iterator());
      if (aggregator != null) {
        aggregator.aggregate(v.getValue());
        if (isAbstractAggregator) {
          AbstractAggregator intern = ((AbstractAggregator) aggregator);
          intern.aggregate(lastValue, v.getValue());
          intern.aggregateInternal();
        }
      }
    }
    iteration++;
  }

  @Override
  public void bsp(BSPPeer peer) throws IOException, SyncException,
      InterruptedException {

    maxIteration = peer.getConfiguration().getInt("hama.graph.max.iteration",
        -1);

    while (updated && !((maxIteration > 0) && iteration > maxIteration)) {
      globalUpdateCounts = 0;
      peer.sync();

      // Map <vertexID, messages>
      final Map<String, LinkedList<Writable>> messages = parseMessages(peer);
      if (isMasterTask(peer) && peer.getSuperstepCount() > 1) {

        MapWritable updatedCnt = new MapWritable();
        // exit if there's no update made
        if (globalUpdateCounts == 0) {
          updatedCnt.put(FLAG_MESSAGE_COUNTS,
              new IntWritable(Integer.MIN_VALUE));
        } else {
          if (aggregator != null) {
            Writable lastAggregatedValue = masterAggregator.getValue();
            if (isAbstractAggregator) {
              final AbstractAggregator intern = ((AbstractAggregator) aggregator);
              final Writable finalizeAggregation = intern.finalizeAggregation();
              if (intern.finalizeAggregation() != null) {
                lastAggregatedValue = finalizeAggregation;
              }
              // this count is usually the times of active vertices in the graph
              updatedCnt.put(FLAG_AGGREGATOR_INCREMENT,
                  intern.getTimesAggregated());
            }
            updatedCnt.put(FLAG_AGGREGATOR_VALUE, lastAggregatedValue);
          }
        }
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, updatedCnt);
        }
      }
      // if we have an aggregator defined, we must make an additional sync
      // to have the updated values available on all our peers.
      if (aggregator != null && peer.getSuperstepCount() > 1) {
        peer.sync();

        MapWritable updatedValues = (MapWritable) peer.getCurrentMessage();
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
      Iterator<Entry<String, LinkedList<Writable>>> iterator = messages
          .entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<String, LinkedList<Writable>> e = iterator.next();
        LinkedList msgs = e.getValue();
        if (combiner != null) {
          Writable combined = combiner.combine(msgs);
          msgs = new LinkedList();
          msgs.add(combined);
        }
        Vertex vertex = vertices.get(e.getKey());
        Writable lastValue = vertex.getValue();
        vertex.compute(msgs.iterator());
        if (aggregator != null) {
          aggregator.aggregate(vertex.getValue());
          if (isAbstractAggregator) {
            AbstractAggregator intern = ((AbstractAggregator) aggregator);
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
              ((AbstractAggregator) aggregator).getTimesAggregated());
        }
      }
      peer.send(masterTask, updatedCnt);
      iteration++;
    }
  }

  private Map<String, LinkedList<Writable>> parseMessages(BSPPeer peer)
      throws IOException {
    MapWritable msg = null;
    Map<String, LinkedList<Writable>> msgMap = new HashMap<String, LinkedList<Writable>>();
    while ((msg = (MapWritable) peer.getCurrentMessage()) != null) {
      for (Entry<Writable, Writable> e : msg.entrySet()) {
        String vertexID = ((Text) e.getKey()).toString();
        if (vertexID.equals(S_FLAG_MESSAGE_COUNTS)) {
          if (((IntWritable) e.getValue()).get() == Integer.MIN_VALUE) {
            updated = false;
          } else {
            globalUpdateCounts += ((IntWritable) e.getValue()).get();
          }
        } else if (aggregator != null
            && vertexID.equals(S_FLAG_AGGREGATOR_VALUE)) {
          masterAggregator.aggregate(e.getValue());
        } else if (aggregator != null
            && vertexID.equals(S_FLAG_AGGREGATOR_INCREMENT)) {
          if (isAbstractAggregator) {
            ((AbstractAggregator) masterAggregator)
                .addTimesAggregated(((IntWritable) e.getValue()).get());
          }
        } else {
          Writable value = e.getValue();
          if (msgMap.containsKey(vertexID)) {
            LinkedList<Writable> msgs = msgMap.get(vertexID);
            msgs.add(value);
            msgMap.put(vertexID, msgs);
          } else {
            LinkedList<Writable> msgs = new LinkedList<Writable>();
            msgs.add(value);
            msgMap.put(vertexID, msgs);
          }
        }
      }
    }
    return msgMap;
  }

  private void loadVertices(BSPPeer peer) throws IOException {
    LOG.debug("vertex class: " + conf.get("hama.graph.vertex.class"));
    boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);
    KeyValuePair<? extends VertexWritable, ? extends VertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      Vertex<? extends Writable> vertex = (Vertex<? extends Writable>) ReflectionUtils
          .newInstance(conf.getClass("hama.graph.vertex.class", Vertex.class),
              conf);

      vertex.setVertexID(next.getKey().getName());
      vertex.peer = peer;
      vertex.runner = this;

      VertexWritable[] arr = (VertexWritable[]) next.getValue().toArray();
      if (selfReference) {
        VertexWritable[] tmp = new VertexWritable[arr.length + 1];
        System.arraycopy(arr, 0, tmp, 0, arr.length);
        tmp[arr.length] = new VertexWritable(vertex.getVertexID());
        arr = tmp;
      }
      List<Edge> edges = new ArrayList<Edge>();
      for (VertexWritable e : arr) {
        String target = peer.getPeerName(Math.abs((e.hashCode() % peer
            .getAllPeerNames().length)));
        edges.add(new Edge(e.getName(), target, e.getWeight()));
      }

      vertex.edges = edges;
      vertex.setup(conf);
      vertices.put(next.getKey().getName(), vertex);
    }
  }

  /**
   * Just write <new Text(vertexID), (Writable) value> pair as a result
   */
  public void cleanup(BSPPeer peer) throws IOException {
    for (Map.Entry<String, Vertex> e : vertices.entrySet()) {
      peer.write(new Text(e.getValue().getVertexID()), e.getValue().getValue());
    }
  }

  private Aggregator<Writable> getNewAggregator() {
    return (Aggregator<Writable>) ReflectionUtils.newInstance(
        conf.getClass("hama.graph.aggregator.class", Aggregator.class), conf);
  }

  private boolean isMasterTask(BSPPeer peer) {
    return peer.getPeerName().equals(masterTask);
  }

  public long getNumberVertices() {
    return numberVertices;
  }

  public long getNumberIterations() {
    return iteration;
  }

  public int getMaxIteration() {
    return maxIteration;
  }

  public Writable getLastAggregatedValue() {
    return globalAggregatorResult;
  }

  public IntWritable getNumLastAggregatedVertices() {
    return globalAggregatorIncrement;
  }

}
