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
package org.apache.hama.examples.graph.partitioning;

import org.apache.hama.examples.graph.ShortestPathVertex;

public class ShortestPathVertexPartitioner extends
    AbstractGraphPartitioner<ShortestPathVertex> {

  @Override
  protected AdjacentPair<ShortestPathVertex> process(String line) {

    String[] vertices = line.split("\t");

    ShortestPathVertex v = new ShortestPathVertex(0, vertices[0]);

    ShortestPathVertex[] adjacents = new ShortestPathVertex[vertices.length - 1];

    for (int i = 1; i < vertices.length; i++) {
      String[] vertexAndWeight = vertices[i].split(":");
      adjacents[i - 1] = new ShortestPathVertex(
          Integer.valueOf(vertexAndWeight[1]), vertexAndWeight[0]);
    }

    return new AdjacentPair<ShortestPathVertex>(v, adjacents);
  }

}
