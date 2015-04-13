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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;

import com.google.common.collect.Sets;

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
  private final Map<V, Vertex<V, E, M>> vertices = new ConcurrentHashMap<V, Vertex<V, E, M>>();

  private Set<V> computedVertices; 
  
  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
  }

  @Override
  public void put(Vertex<V, E, M> vertex) throws IOException {
    if(vertices.containsKey(vertex.getVertexID())) {
      for(Edge<V, E> e : vertex.getEdges()) 
      vertices.get(vertex.getVertexID()).addEdge(e);
    } else {
      vertices.put(vertex.getVertexID(), vertex);
    }
  }

  @Override
  public void remove(V vertexID) throws UnsupportedOperationException {
    vertices.remove(vertexID);
  }

  public void clear() {
    vertices.clear();
  }

  @Override
  public Collection<Vertex<V, E, M>> getValues() {
    return vertices.values();  
  }
  
  @Override
  public int size() {
    return vertices.size();
  }

  @Override
  public Vertex<V, E, M> get(V vertexID) {
    return vertices.get(vertexID);
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
        return vertexIterator.next();
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
  public synchronized void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException {
    computedVertices.add(vertex.getVertexID());
  }

  @Override
  public void finishAdditions() {
  }

  @Override
  public void finishRemovals() {
  }

  @Override
  public void startSuperstep() throws IOException {
    computedVertices = new HashSet<V>();
  }

  @Override
  public void finishSuperstep() throws IOException {
  }

  @Override
  public Set<V> getComputedVertices() {
    return this.computedVertices;
  }
  
  public Set<V> getNotComputedVertices() {
    return Sets.difference(vertices.keySet(), computedVertices);
  }

  public int getActiveVerticesNum() {
    return computedVertices.size();
  }
}
