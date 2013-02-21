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
import org.apache.hadoop.io.WritableComparable;

/**
 * VerticesInfo encapsulates the storage of vertices in a BSP Task.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
public final class ListVerticesInfo<V extends WritableComparable<V>, E extends Writable, M extends Writable>
    implements IVerticesInfo<V, E, M> {

  private final List<Vertex<V, E, M>> vertices = new ArrayList<Vertex<V, E, M>>(
      100);

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.graph.IVerticesInfo#addVertex(org.apache.hama.graph.Vertex)
   */
  @Override
  public void addVertex(Vertex<V, E, M> vertex) {
    vertices.add(vertex);
  }

  public void clear() {
    vertices.clear();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.graph.IVerticesInfo#size()
   */
  @Override
  public int size() {
    return this.vertices.size();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.graph.IVerticesInfo#iterator()
   */
  @Override
  public Iterator<Vertex<V, E, M>> iterator() {
    return vertices.iterator();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.graph.IVerticesInfo#recoverState(java.io.DataInput)
   */
  @Override
  public void recoverState(DataInput in) {

  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.graph.IVerticesInfo#saveState(java.io.DataOutput)
   */
  @Override
  public void saveState(DataOutput out) {

  }
}
