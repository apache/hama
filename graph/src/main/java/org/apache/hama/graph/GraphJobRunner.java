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
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;

/**
 * Fully generic graph job runner.
 * 
 * @param <V> the id type of a vertex.
 * @param <E> the value type of an edge.
 * @param <M> the value type of a vertex.
 */
public final class GraphJobRunner<V extends Writable, E extends Writable, M extends Writable>
    extends GraphJobRunnerBase<V, E, M> {

  @Override
  @SuppressWarnings("unchecked")
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException, SyncException, InterruptedException {
    this.peer = peer;
    this.conf = peer.getConfiguration();
    // Choose one as a master to collect global updates
    this.masterTask = peer.getPeerName(0);
    maxIteration = peer.getConfiguration().getInt("hama.graph.max.iteration",
        -1);

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

    loadVertices(peer, repairNeeded, runtimePartitioning, partitioner, reader,
        this);

    for (String peerName : peer.getAllPeerNames()) {
      peer.send(peerName, new GraphJobMessage(new IntWritable(vertices.size())));
    }

    peer.sync();

    GraphJobMessage msg = null;
    while ((msg = peer.getCurrentMessage()) != null) {
      if (msg.isVerticesSizeMessage()) {
        numberVertices += msg.getVerticesSize().get();
      }
    }

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

}
