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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * VerticesInfo encapsulates the storage of vertices in a BSP Task.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
public class VerticesInfo<V extends Writable, E extends Writable, M extends Writable>
    implements Iterable<Vertex<V, E, M>> {

  private List<Vertex<V, E, M>> vertices = new ArrayList<Vertex<V, E, M>>(100);

  public void addVertex(Vertex<V, E, M> vertex) {
    int i = 0;
    for (Vertex<V, E, M> check : this) {
      if (check.getVertexID().equals(vertex.getVertexID())) {
        this.vertices.set(i, vertex);
        return;
      }
      ++i;
    }
    vertices.add(vertex);
  }

  public Vertex<V, E, M> getVertex(V vertexId) {
    for (Vertex<V, E, M> vertex : this) {
      if (vertex.getVertexID().equals(vertexId)) {
        return vertex;
      }
    }
    return null;
  }

  public boolean containsVertex(V vertexId) {
    for (Vertex<V, E, M> vertex : this) {
      if (vertex.getVertexID().equals(vertexId)) {
        return true;
      }
    }
    return false;
  }

  public void clear() {
    vertices.clear();
  }

  public int size() {
    return this.vertices.size();
  }

  @Override
  public Iterator<Vertex<V, E, M>> iterator() {
    return vertices.iterator();
  }

  public void recoverState(DataInput in) {

  }

  public void saveState(DataOutput out) {

  }
}
