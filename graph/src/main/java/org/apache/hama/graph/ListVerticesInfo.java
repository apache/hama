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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  private final List<Vertex<V, E, M>> vertices = new ArrayList<Vertex<V, E, M>>(
      100);

  @Override
  public void addVertex(Vertex<V, E, M> vertex) {
    vertices.add(vertex);
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
      int currentIndex = 0;

      @Override
      public boolean hasNext(V e,
          org.apache.hama.graph.IDSkippingIterator.Strategy strat) {
        if (currentIndex < vertices.size()) {

          while (!strat.accept(vertices.get(currentIndex), e)) {
            currentIndex++;
          }

          return true;
        } else {
          return false;
        }
      }

      @Override
      public Vertex<V, E, M> next() {
        return vertices.get(currentIndex++);
      }

    };
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex) {

  }

  @Override
  public void finishAdditions() {
    Collections.sort(vertices);
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
