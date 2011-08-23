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

import org.apache.hama.examples.graph.Vertex;

public class VertexPartitioner extends AbstractGraphPartitioner<Vertex> {

  @Override
  protected AdjacentPair<Vertex> process(String line) {

    String[] vertices = line.split("\t");

    Vertex v = new Vertex(vertices[0]);
    Vertex[] adjacents = new Vertex[vertices.length - 1];

    for (int i = 1; i < vertices.length; i++) {
      adjacents[i - 1] = new Vertex(vertices[i]);
    }

    return new AdjacentPair<Vertex>(v, adjacents);
  }
  
  
}
