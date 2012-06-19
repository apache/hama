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

import org.apache.hadoop.io.DoubleWritable;

/**
 * A absolute difference aggregator, it collects values before the compute and
 * after the compute, then calculates the difference and globally accumulates
 * (sums them up) them.
 */
public class AbsDiffAggregator extends
    AbstractAggregator<DoubleWritable, Vertex<?, ?, DoubleWritable>> {

  double absoluteDifference = 0.0d;

  @Override
  public void aggregate(Vertex<?, ?, DoubleWritable> v,
      DoubleWritable oldValue, DoubleWritable newValue) {
    // make sure it's nullsafe
    if (oldValue != null) {
      absoluteDifference += Math.abs(oldValue.get() - newValue.get());
    }
  }

  // when a master aggregates he aggregated values, he calls this, so let's just
  // sum up here.
  @Override
  public void aggregate(Vertex<?, ?, DoubleWritable> vertex,
      DoubleWritable value) {
    absoluteDifference += value.get();
  }

  @Override
  public DoubleWritable getValue() {
    return new DoubleWritable(absoluteDifference);
  }

}
