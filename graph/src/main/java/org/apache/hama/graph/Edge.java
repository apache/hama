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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * The edge class
 */
public final class Edge<VERTEX_ID extends Writable, EDGE_VALUE_TYPE extends Writable> {

  private final VERTEX_ID destinationVertexID;
  private final EDGE_VALUE_TYPE cost;

  public Edge(VERTEX_ID sourceVertexID, EDGE_VALUE_TYPE cost) {
    this.destinationVertexID = sourceVertexID;
    if (cost == null || cost instanceof NullWritable) {
      this.cost = null;
    } else {
      this.cost = cost;
    }
  }

  public VERTEX_ID getDestinationVertexID() {
    return destinationVertexID;
  }

  public EDGE_VALUE_TYPE getValue() {
    return cost;
  }

  @Override
  public String toString() {
    return this.destinationVertexID + ":" + this.getValue();
  }
}
