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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * VerticesInfo encapsulates the storage of vertices in a BSP Task.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
public final class ListVerticesInfo<V extends WritableComparable<V>, E extends Writable, M extends Writable>
    implements VerticesInfo<V, E, M> {

  private final SortedSet<Vertex<V, E, M>> vertices = new TreeSet<Vertex<V, E, M>>();
  // We will use this variable to make vertex removals, so we don't invoke GC too many times. 
  private final Vertex<V, E, M> vertexTemplate = GraphJobRunner.<V, E, M> newVertexInstance(GraphJobRunner.VERTEX_CLASS);

  @Override
  public void addVertex(Vertex<V, E, M> vertex) {
    if (!vertices.add(vertex)) {
      throw new UnsupportedOperationException("Vertex with ID: " + vertex.getVertexID() + " already exists!");
    }
  }

  @Override
  public void removeVertex(V vertexID) throws UnsupportedOperationException {
    vertexTemplate.setVertexID(vertexID);    
    
    if (!vertices.remove(vertexTemplate)) {
      throw new UnsupportedOperationException("Vertex with ID: " + vertexID + " not found on this peer.");
    }
  }

  public void clear() {
    vertices.clear();
  }

  @Override
  public int size() {
    return this.vertices.size();
  }

  @Override
  public IDSkippingIterator<V, E, M> skippingIterator() {
    return new IDSkippingIterator<V, E, M>() {
      Iterator<Vertex<V, E, M>> it = vertices.iterator();
      Vertex<V, E, M> v;

      @Override
      public boolean hasNext(V msgId,
          org.apache.hama.graph.IDSkippingIterator.Strategy strat) {

        if (it.hasNext()) {
          v = it.next();

          while (!strat.accept(v, msgId)) {
            if (it.hasNext()) {
              v = it.next();
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
          throw new UnsupportedOperationException("You must invoke hasNext before ask for the next vertex.");
        }

        Vertex<V, E, M> tmp = v;
        v = null;
        return tmp;
      }

    };
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex) {

  }

  @Override
  public void finishAdditions() {

  }

  @Override
  public void finishRemovals() {
  }

  @Override
  public boolean isFinishedAdditions() {
    return false;
  }

  @Override
  public void finishSuperstep() {

  }

  @Override
  public void cleanup(Configuration conf, TaskAttemptID attempt)
      throws IOException {

  }

  @Override
  public void startSuperstep() throws IOException {

  }

  @Override
  public void init(GraphJobRunner<V, E, M> runner, Configuration conf,
      TaskAttemptID attempt) throws IOException {

  }

}
