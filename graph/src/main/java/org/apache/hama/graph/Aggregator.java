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

import org.apache.hadoop.io.Writable;

/**
 * Aggregators are a mechanism for global communication, monitoring, and data.
 * Each vertex can provide a value to an aggregator in superstep S, the system
 * combines those values using a reduction operator, and the resulting value is
 * made available to all vertices in superstep S + 1. <br/>
 * The result of an aggregator from the last superstep can be picked up by the
 * vertex itself via {@link Vertex}#getLastAggregatedValue();
 */
public interface Aggregator<M extends Writable, VERTEX extends Vertex<?, ?, ?>> {

  /**
   * Observes a new vertex value.
   */
  public void aggregate(VERTEX vertex, M value);

  /**
   * Gets a vertex value.
   */
  public M getValue();

}
