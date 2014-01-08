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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

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
  Vertex<V, E, M> v;

  private final SortedMap<V, byte[]> verticesMap = new TreeMap<V, byte[]>();

  private ByteArrayOutputStream bos = null;
  private DataOutputStream dos = null;
  private ByteArrayInputStream bis = null;
  private DataInputStream dis = null;

  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
    this.runner = runner;
  }

  @Override
  public void addVertex(Vertex<V, E, M> vertex) throws IOException {
    if (verticesMap.containsKey(vertex.getVertexID())) {
      throw new UnsupportedOperationException("Vertex with ID: "
          + vertex.getVertexID() + " already exists!");
    } else {
      verticesMap.put(vertex.getVertexID(), serialize(vertex));
    }
  }

  @Override
  public void removeVertex(V vertexID) throws UnsupportedOperationException {
    if (verticesMap.containsKey(vertexID)) {
      verticesMap.remove(vertexID);
    } else {
      throw new UnsupportedOperationException("Vertex with ID: " + vertexID
          + " not found on this peer.");
    }
  }

  public void clear() {
    verticesMap.clear();
  }

  @Override
  public int size() {
    return this.verticesMap.size();
  }

  @Override
  public IDSkippingIterator<V, E, M> skippingIterator() {
    return new IDSkippingIterator<V, E, M>() {
      Iterator<V> it = verticesMap.keySet().iterator();

      @Override
      public boolean hasNext(V msgId,
          org.apache.hama.graph.IDSkippingIterator.Strategy strat)
          throws IOException {

        if (it.hasNext()) {
          V vertexID = it.next();
          v = deserialize(vertexID, verticesMap.get(vertexID));

          while (!strat.accept(v, msgId)) {
            if (it.hasNext()) {
              vertexID = it.next();
              v = deserialize(vertexID, verticesMap.get(vertexID));
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

  public byte[] serialize(Vertex<V, E, M> vertex) throws IOException {
    bos = new ByteArrayOutputStream();
    dos = new DataOutputStream(bos);
    vertex.write(dos);
    return bos.toByteArray();
  }

  public Vertex<V, E, M> deserialize(V vertexID, byte[] serialized)
      throws IOException {
    bis = new ByteArrayInputStream(serialized);
    dis = new DataInputStream(bis);
    v = GraphJobRunner.<V, E, M> newVertexInstance(GraphJobRunner.VERTEX_CLASS);

    v.readFields(dis);
    v.setRunner(runner);
    v.setVertexID(vertexID);
    return v;
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException {
    verticesMap.put(vertex.getVertexID(), serialize(vertex));
  }

  @Override
  public void finishAdditions() {
  }

  @Override
  public void finishRemovals() {
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

  }
}
