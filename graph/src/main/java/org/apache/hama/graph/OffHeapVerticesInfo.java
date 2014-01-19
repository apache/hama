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

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.memory.Pointer;
import org.apache.directmemory.serialization.Serializer;
import org.apache.directmemory.serialization.kryo.KryoSerializer;
import org.apache.directmemory.utils.CacheValuesIterable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.util.ReflectionUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * An off heap version of a {@link org.apache.hama.graph.Vertex} storage.
 */
public class OffHeapVerticesInfo<V extends WritableComparable<?>, E extends Writable, M extends Writable>
    implements VerticesInfo<V, E, M> {

  public static final String DM_STRICT_ITERATOR = "dm.iterator.strict";
  public static final String DM_BUFFERS = "dm.buffers";
  public static final String DM_SIZE = "dm.size";
  public static final String DM_CAPACITY = "dm.capacity";
  public static final String DM_CONCURRENCY = "dm.concurrency";
  public static final String DM_DISPOSAL_TIME = "dm.disposal.time";
  public static final String DM_SERIALIZER = "dm.serializer";
  public static final String DM_SORTED = "dm.sorted";

  private CacheService<V, Vertex<V, E, M>> vertices;

  private boolean strict;
  private GraphJobRunner<V, E, M> runner;

  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
    this.runner = runner;
    this.strict = conf.getBoolean(DM_STRICT_ITERATOR, true);
    DirectMemory<V, Vertex<V, E, M>> dm = new DirectMemory<V, Vertex<V, E, M>>()
        .setNumberOfBuffers(conf.getInt(DM_BUFFERS, 100))
        .setSize(conf.getInt(DM_SIZE, 102400))
        .setSerializer(
            ReflectionUtils.newInstance(conf.getClass(DM_SERIALIZER,
                KryoSerializer.class, Serializer.class)))
        .setDisposalTime(conf.getInt(DM_DISPOSAL_TIME, 3600000));
    if (conf.getBoolean(DM_SORTED, true)) {
      dm.setMap(new ConcurrentSkipListMap<V, Pointer<Vertex<V, E, M>>>());
    } else {
      dm.setInitialCapacity(conf.getInt(DM_CAPACITY, 1000))
          .setConcurrencyLevel(conf.getInt(DM_CONCURRENCY, 10));
    }

    this.vertices = dm.newCacheService();

  }

  @Override
  public void cleanup(HamaConfiguration conf, TaskAttemptID attempt)
      throws IOException {
    vertices.dump();
  }

  @Override
  public void addVertex(Vertex<V, E, M> vertex) {
    vertices.put(vertex.getVertexID(), vertex);
  }

  @Override
  public void finishAdditions() {
  }

  @Override
  public void startSuperstep() throws IOException {
  }

  @Override
  public void finishSuperstep() throws IOException {
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException {
    vertices.put(vertex.getVertexID(), vertex);
  }

  public void clear() {
    vertices.clear();
  }

  @Override
  public int size() {
    return (int) this.vertices.entries();
  }

  @Override
  public IDSkippingIterator<V, E, M> skippingIterator() {
    final Iterator<Vertex<V, E, M>> vertexIterator = new CacheValuesIterable<V, Vertex<V, E, M>>(
        vertices, strict).iterator();

    return new IDSkippingIterator<V, E, M>() {
      int currentIndex = 0;

      Vertex<V, E, M> currentVertex = null;

      @Override
      public boolean hasNext(V e,
          org.apache.hama.graph.IDSkippingIterator.Strategy strat) {
        if (currentIndex < vertices.entries()) {

          Vertex<V, E, M> next = vertexIterator.next();
          while (!strat.accept(next, e)) {
            currentIndex++;
          }
          currentVertex = next;
          return true;
        } else {
          return false;
        }
      }

      @Override
      public Vertex<V, E, M> next() {
        currentIndex++;
        if (currentVertex != null && currentVertex.getRunner() == null) {
          currentVertex.setRunner(runner);
        }
        return currentVertex;
      }

    };

  }

  @Override
  public void removeVertex(V vertexID) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void finishRemovals() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

}
