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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Abstract base case of an aggregator. Also defines a sticky operation that
 * aggregates the last value and the new one of a vertex. <br/>
 * For tracking cases it increments an internal counter on each call of
 * aggregate.
 */
public abstract class AbstractAggregator<M extends Writable, VERTEX extends Vertex<?, ?, M>>
    implements Aggregator<M, VERTEX> {

  private int timesAggregated = 0;

  /**
   * Used for internal tracking purposes.
   */
  void aggregateInternal() {
    timesAggregated++;
  }

  /**
   * Used for internal summing.
   */
  void addTimesAggregated(int val) {
    timesAggregated += val;
  }

  /**
   * Observes a value of a vertex after the compute method. This is intended to
   * be overriden by the user and is just an empty implementation in this class.
   * Please make sure that you are null-checking vertex, since on a master task
   * this will always be null.
   */
  @Override
  public void aggregate(VERTEX vertex, M value) {

  }

  /**
   * Observes the old value of a vertex and the new value at the same time. This
   * is intended to be overridden by the user and is just an empty
   * implementation in this class.Please make sure that you are null-checking
   * vertex, since on a master task this will always be null.
   */
  public void aggregate(VERTEX vertex, M oldValue, M newValue) {

  }

  /**
   * Finalizes the aggregation on a master task. This is intended to be
   * overridden by the user and is just an empty implementation in this class
   * (returns null).
   */
  public M finalizeAggregation() {
    return null;
  }

  /**
   * Gets the value of the aggregator. This is intended to be overridden by the
   * user and is just an empty implementation in this class (returns null).
   */
  @Override
  public M getValue() {
    return null;
  }

  public IntWritable getTimesAggregated() {
    return new IntWritable(timesAggregated);
  }
  
  @Override
  public String toString() {
    return "VAL=" + getValue();
  }

}
