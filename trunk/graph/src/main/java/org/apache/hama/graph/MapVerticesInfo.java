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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.util.WritableUtils;

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
  private final ConcurrentHashMap<V, byte[]> vertices = new ConcurrentHashMap<V, byte[]>();

  private GraphJobRunner<V, E, M> runner;
  private HamaConfiguration conf;
  private AtomicInteger activeVertices = new AtomicInteger(0);

  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
    this.runner = runner;
    this.conf = conf;
  }

  @Override
  public void put(Vertex<V, E, M> vertex) throws IOException {
    if (!vertices.containsKey(vertex.getVertexID())) {
      if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
        vertices.putIfAbsent(vertex.getVertexID(),
            WritableUtils.serialize(vertex));
      } else {
        vertices.putIfAbsent(vertex.getVertexID(),
            WritableUtils.unsafeSerialize(vertex));
      }
    } else {
      Vertex<V, E, M> v = this.get(vertex.getVertexID());
      for (Edge<V, E> e : vertex.getEdges()) {
        v.addEdge(e);
      }
      if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
        vertices.put(vertex.getVertexID(), WritableUtils.serialize(v));
      } else {
        vertices.put(vertex.getVertexID(), WritableUtils.unsafeSerialize(v));
      }
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
  public int size() {
    return vertices.size();
  }

  @Override
  public Vertex<V, E, M> get(V vertexID) throws IOException {
    Vertex<V, E, M> v = GraphJobRunner
        .<V, E, M> newVertexInstance(GraphJobRunner.VERTEX_CLASS);
    if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
      WritableUtils.deserialize(vertices.get(vertexID), v);
    } else {
      WritableUtils.unsafeDeserialize(vertices.get(vertexID), v);
    }
    v.setRunner(runner);

    return v;
  }

  @Override
  public Iterator<Vertex<V, E, M>> iterator() {

    final Iterator<byte[]> it = vertices.values().iterator();

    return new Iterator<Vertex<V, E, M>>() {

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public Vertex<V, E, M> next() {
        Vertex<V, E, M> v = GraphJobRunner
            .<V, E, M> newVertexInstance(GraphJobRunner.VERTEX_CLASS);

        if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
          WritableUtils.deserialize(it.next(), v);
        } else {
          WritableUtils.unsafeDeserialize(it.next(), v);
        }

        v.setRunner(runner);
        return v;
      }

      @Override
      public void remove() {
        it.remove();
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
    incrementCount();
    vertex.setComputed();
    if (!conf.getBoolean("hama.use.unsafeserialization", false)) {
      vertices.put(vertex.getVertexID(), WritableUtils.serialize(vertex));
    } else {
      vertices.put(vertex.getVertexID(), WritableUtils.unsafeSerialize(vertex));
    }
  }

  public void incrementCount() {
    activeVertices.incrementAndGet();
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
    activeVertices.set(0);
  }

  public int getActiveVerticesNum() {
    return activeVertices.get();
  }
}
