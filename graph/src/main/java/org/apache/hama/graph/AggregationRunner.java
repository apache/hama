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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;

/**
 * Runner class to do the tasks that need to be done if aggregation was
 * configured.
 * 
 */
@SuppressWarnings("rawtypes")
public final class AggregationRunner<V extends WritableComparable, E extends Writable, M extends Writable> {
  private Map<String, Aggregator> Aggregators;
  private Map<String, Writable> aggregatorResults;
  private Set<String> aggregatorsUsed;

  private Configuration conf;
  private Text textWrap = new Text();

  public void setupAggregators(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    this.conf = peer.getConfiguration();

    this.aggregatorResults = new HashMap<String, Writable>(4);
    this.Aggregators = new HashMap<String, Aggregator>(4);
    this.aggregatorsUsed = new HashSet<String>(4);

    String customAggregatorClasses = peer.getConfiguration().get(
        GraphJob.AGGREGATOR_CLASS_ATTR);

    if (customAggregatorClasses != null) {
      String[] custAggrs = customAggregatorClasses.split(";");

      for (String aggr : custAggrs) {
        String[] Name_AggrClass = aggr.split("@", 2);
        this.Aggregators.put(Name_AggrClass[0],
            getNewAggregator(Name_AggrClass[1]));
      }
    }
  }

  /**
   * The method the master task does, it globally aggregates the values of each
   * peer and updates the given map accordingly.
   */
  public void doMasterAggregation(MapWritable updatedCnt) {
    // Get results only from used aggregators.
    for (String name : this.aggregatorsUsed) {
      updatedCnt.put(new Text(name), this.Aggregators.get(name).getValue());
    }
    this.aggregatorsUsed.clear();

    // Reset all custom aggregators. TODO: Change the aggregation interface to
    // include clean() method.
    Map<String, Aggregator> tmp = new HashMap<String, Aggregator>(4);
    for (Entry<String, Aggregator> e : this.Aggregators.entrySet()) {
      String aggClass = e.getValue().getClass().getName();
      tmp.put(e.getKey(), getNewAggregator(aggClass));
    }
    this.Aggregators = tmp;
  }

  /**
   * Receives aggregated values from a master task.
   * 
   * @return always true if no aggregators are defined, false if aggregators say
   *         we haven't seen any messages anymore.
   */
  public boolean receiveAggregatedValues(MapWritable updatedValues,
      long iteration) {
    // In every superstep, we create a new result collection as we don't save
    // history.
    // If a value is missing, the user will take a null result. By creating a
    // new collection
    // every time, we can reduce the network cost (because we send less
    // information by skipping null values)
    // But we are losing in GC.
    this.aggregatorResults = new HashMap<String, Writable>(4);
    for (String name : this.Aggregators.keySet()) {
      this.textWrap.set(name);
      this.aggregatorResults.put(name, updatedValues.get(textWrap));
    }

    IntWritable count = (IntWritable) updatedValues
        .get(GraphJobRunner.FLAG_MESSAGE_COUNTS);
    if (count != null && count.get() == Integer.MIN_VALUE) {
      return false;
    }
    return true;
  }

  /**
   * Method to let the custom master aggregator read messages from peers and
   * aggregate a value.
   */
  @SuppressWarnings("unchecked")
  public void masterAggregation(Text name, Writable value) {
    String nameIdx = name.toString().split(";", 2)[1];
    this.Aggregators.get(nameIdx).aggregate(null, value);

    // When it's time to send the values, we can see which aggregators are used.
    this.aggregatorsUsed.add(nameIdx);
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

  public final Writable getAggregatedValue(String name) {
    return this.aggregatorResults.get(name);
  }
}
