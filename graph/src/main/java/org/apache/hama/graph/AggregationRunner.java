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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import com.google.common.base.Preconditions;

/**
 * Runner class to do the tasks that need to be done if aggregation was
 * configured.
 * 
 */
public final class AggregationRunner<V extends Writable, E extends Writable, M extends Writable> {

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

  private boolean enabled = false;
  private Configuration conf;

  @SuppressWarnings("unchecked")
  public void setupAggregators(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    this.conf = peer.getConfiguration();
    String aggregatorClasses = peer.getConfiguration().get(
        GraphJob.AGGREGATOR_CLASS_ATTR);
    if (aggregatorClasses != null) {
      enabled = true;
      aggregatorClassNames = aggregatorClasses.split(";");
      // init to the split size
      aggregators = new Aggregator[aggregatorClassNames.length];
      globalAggregatorResult = new Writable[aggregatorClassNames.length];
      globalAggregatorIncrement = new IntWritable[aggregatorClassNames.length];
      isAbstractAggregator = new boolean[aggregatorClassNames.length];
      aggregatorValueFlag = new Text[aggregatorClassNames.length];
      aggregatorIncrementFlag = new Text[aggregatorClassNames.length];
      if (GraphJobRunner.isMasterTask(peer)) {
        masterAggregator = new Aggregator[aggregatorClassNames.length];
      }
      for (int i = 0; i < aggregatorClassNames.length; i++) {
        aggregators[i] = getNewAggregator(aggregatorClassNames[i]);
        aggregatorValueFlag[i] = new Text(
            GraphJobRunner.S_FLAG_AGGREGATOR_VALUE + ";" + i);
        aggregatorIncrementFlag[i] = new Text(
            GraphJobRunner.S_FLAG_AGGREGATOR_INCREMENT + ";" + i);
        if (aggregators[i] instanceof AbstractAggregator) {
          isAbstractAggregator[i] = true;
        }
        if (GraphJobRunner.isMasterTask(peer)) {
          masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
        }
      }
    }
  }

  /**
   * Runs the aggregators by sending their values to the master task.
   */
  public void sendAggregatorValues(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      int activeVertices) throws IOException {
    // send msgCounts to the master task
    MapWritable updatedCnt = new MapWritable();
    updatedCnt.put(GraphJobRunner.FLAG_MESSAGE_COUNTS, new IntWritable(
        activeVertices));
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
        if (GraphJobRunner.isMasterTask(peer)) {
          masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
        }
      }
    }
    peer.send(GraphJobRunner.getMasterTask(peer), new GraphJobMessage(
        updatedCnt));
  }

  /**
   * Aggregates the last value before computation and the value after the
   * computation.
   * 
   * @param lastValue the value before compute().
   * @param v the vertex.
   */
  public void aggregateVertex(M lastValue, Vertex<V, E, M> v) {
    if (isEnabled()) {
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

  /**
   * The method the master task does, it globally aggregates the values of each
   * peer and updates the given map accordingly.
   */
  public void doMasterAggregation(MapWritable updatedCnt) {
    if (isEnabled()) {
      // work through the master aggregators
      for (int i = 0; i < masterAggregator.length; i++) {
        Writable lastAggregatedValue = masterAggregator[i].getValue();
        if (isAbstractAggregator[i]) {
          final AbstractAggregator<M, Vertex<V, E, M>> intern = ((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[i]);
          final Writable finalizeAggregation = intern.finalizeAggregation();
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

  /**
   * Receives aggregated values from a master task, by doing an additional
   * barrier sync and parsing the messages.
   * 
   * @return always true if no aggregators are defined, false if aggregators say
   *         we haven't seen any messages anymore.
   */
  public boolean receiveAggregatedValues(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      long iteration) throws IOException, SyncException, InterruptedException {
    // if we have an aggregator defined, we must make an additional sync
    // to have the updated values available on all our peers.
    if (isEnabled() && iteration > 1) {
      peer.sync();

      MapWritable updatedValues = peer.getCurrentMessage().getMap();
      for (int i = 0; i < aggregators.length; i++) {
        globalAggregatorResult[i] = updatedValues.get(aggregatorValueFlag[i]);
        globalAggregatorIncrement[i] = (IntWritable) updatedValues
            .get(aggregatorIncrementFlag[i]);
      }
      IntWritable count = (IntWritable) updatedValues
          .get(GraphJobRunner.FLAG_MESSAGE_COUNTS);
      if (count != null && count.get() == Integer.MIN_VALUE) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if aggregators were defined. Normally used by the internal
   *         stateful methods, outside shouldn't use it too extensively.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Method to let the master read messages from peers and aggregate a value.
   */
  public void masterReadAggregatedValue(Text textIndex, M value) {
    int index = Integer.parseInt(textIndex.toString().split(";")[1]);
    masterAggregator[index].aggregate(null, value);
  }

  /**
   * Method to let the master read messages from peers and aggregate the
   * incremental value.
   */
  public void masterReadAggregatedIncrementalValue(Text textIndex, M value) {
    int index = Integer.parseInt(textIndex.toString().split(";")[1]);
    if (isAbstractAggregator[index]) {
      ((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[index])
          .addTimesAggregated(((IntWritable) value).get());
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

  public final Writable getLastAggregatedValue(int index) {
    return globalAggregatorResult[Preconditions.checkPositionIndex(index,
        globalAggregatorResult.length)];
  }

  public final IntWritable getNumLastAggregatedVertices(int index) {
    return globalAggregatorIncrement[Preconditions.checkPositionIndex(index,
        globalAggregatorIncrement.length)];
  }
}
