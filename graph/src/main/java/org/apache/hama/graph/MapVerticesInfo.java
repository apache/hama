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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Stores the vertices into a memory-based tree map. This implementation allows
 * the runtime graph modification and random access by vertex ID.
 * 
 * But it might be inefficient in memory usage.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
public final class MapVerticesInfo<V extends WritableComparable<V>, E extends Writable, M extends Writable>
    implements VerticesInfo<V, E, M> {
  private GraphJobRunner<V, E, M> runner;

  private final Map<V, Vertex<V, E, M>> vertices = new HashMap<V, Vertex<V, E, M>>();

  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
    this.runner = runner;
  }

  @Override
  public void put(Vertex<V, E, M> vertex) throws IOException {
    vertices.put(vertex.getVertexID(), vertex);
  }

  @Override
  public void remove(V vertexID) throws UnsupportedOperationException {
    vertices.remove(vertexID);
  }

  public void clear() {
    vertices.clear();
  }

  @Override
  public int size() {
    return vertices.size();
  }

  @Override
  public Vertex<V, E, M> get(V vertexID) {
    Vertex<V, E, M> vertex = vertices.get(vertexID);
    vertex.setRunner(runner);
    return vertex;
  }

  @Override
  public Iterator<Vertex<V, E, M>> iterator() {

    final Iterator<Vertex<V, E, M>> vertexIterator = vertices.values().iterator();
    
    return new Iterator<Vertex<V, E, M>>() {

      @Override
      public boolean hasNext() {
        return vertexIterator.hasNext();
      }

      @Override
      public Vertex<V, E, M> next() {
        Vertex<V, E, M> vertex = vertexIterator.next();
        vertex.setRunner(runner);
        return vertex;
      }

      @Override
      public void remove() {
        // TODO Auto-generated method stub
      }

    };
  }

  @Override
  public Set<V> keySet() {
    return vertices.keySet();
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException {
    vertices.put(vertex.getVertexID(), vertex);
  }

  @Override
  public void finishAdditions() {
  }

  @Override
  public void finishRemovals() {
  }

  @Override
  public void startSuperstep() throws IOException {
  }

  @Override
  public void finishSuperstep() throws IOException {
  }

}
