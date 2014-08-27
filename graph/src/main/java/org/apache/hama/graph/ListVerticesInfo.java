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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.util.KryoSerializer;

/**
 * Stores the serialized vertices into a memory-based list. It doesn't allow
 * modification and random access by vertexID.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
public final class ListVerticesInfo<V extends WritableComparable<V>, E extends Writable, M extends Writable>
    implements VerticesInfo<V, E, M> {
  private GraphJobRunner<V, E, M> runner;
  Vertex<V, E, M> v;

  private final List<byte[]> verticesList = new ArrayList<byte[]>();
  private boolean lockedAdditions = false;
  private int index = 0;

  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
    this.runner = runner;
  }

  @Override
  public void addVertex(Vertex<V, E, M> vertex) throws IOException {
    // messages must be added in sorted order to work this out correctly
    checkArgument(!lockedAdditions,
        "Additions are locked now, nobody is allowed to change the structure anymore.");

    verticesList.add(serialize(vertex));
  }

  @Override
  public void removeVertex(V vertexID) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "ListVerticesInfo doesn't support this operation. Please use the MapVerticesInfo.");
  }

  public void clear() {
    verticesList.clear();
  }

  @Override
  public int size() {
    return this.verticesList.size();
  }

  @Override
  public IDSkippingIterator<V, E, M> skippingIterator() {
    return new IDSkippingIterator<V, E, M>() {
      Iterator<byte[]> it = verticesList.iterator();

      @Override
      public boolean hasNext(V msgId,
          org.apache.hama.graph.IDSkippingIterator.Strategy strat)
          throws IOException {

        if (it.hasNext()) {
          byte[] serialized = it.next();
          v = deserialize(serialized);

          while (!strat.accept(v, msgId)) {
            if (it.hasNext()) {
              serialized = it.next();
              v = deserialize(serialized);
            } else {
              return false;
            }
          }

          return true;
        } else {
          v = null;
          return false;
        }
      }

      @Override
      public Vertex<V, E, M> next() {
        if (v == null) {
          throw new UnsupportedOperationException(
              "You must invoke hasNext before ask for the next vertex.");
        }

        Vertex<V, E, M> tmp = v;
        v = null;
        return tmp;
      }

    };
  }

  private final KryoSerializer kryo = new KryoSerializer(GraphJobRunner.VERTEX_CLASS);

  public byte[] serialize(Vertex<V, E, M> vertex) throws IOException {
    return kryo.serialize(vertex);
  }

  @SuppressWarnings("unchecked")
  public Vertex<V, E, M> deserialize(byte[] serialized) throws IOException {
    v = (Vertex<V, E, M>) kryo.deserialize(serialized);
    v.setRunner(runner);
    return v;
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException {
    verticesList.set(index, serialize(vertex));
    index++;
  }

  @Override
  public void finishAdditions() {
    lockedAdditions = true;
  }

  @Override
  public void finishRemovals() {
    throw new UnsupportedOperationException(
        "ListVerticesInfo doesn't support this operation. Please use the MapVerticesInfo.");
  }

  @Override
  public void finishSuperstep() {

  }

  @Override
  public void cleanup(HamaConfiguration conf, TaskAttemptID attempt)
      throws IOException {

  }

  @Override
  public void startSuperstep() throws IOException {
    index = 0;
  }
}
