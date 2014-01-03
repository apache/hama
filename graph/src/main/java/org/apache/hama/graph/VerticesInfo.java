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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * VerticesInfo interface encapsulates the storage of vertices in a BSP Task.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
@SuppressWarnings("rawtypes")
public interface VerticesInfo<V extends WritableComparable, E extends Writable, M extends Writable> {

  /**
   * Initialization of internal structures.
   */
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException;

  /**
   * Cleanup of internal structures.
   */
  public void cleanup(HamaConfiguration conf, TaskAttemptID attempt)
      throws IOException;

  /**
   * Add a vertex to the underlying structure.
   */
  public void addVertex(Vertex<V, E, M> vertex) throws IOException;

  /**
   * Remove a vertex to the underlying structure.
   */
  public void removeVertex(V vertexID) throws UnsupportedOperationException;

  /**
   * Finish the additions, from this point on the implementations should close
   * the adds and throw exceptions in case something is added after this call.
   */
  public void finishAdditions();

  /**
   * Finish the removals, from this point on the implementations should close
   * the removes and throw exceptions in case something is removed after this call.
   */
  public void finishRemovals();

  /**
   * Called once a superstep starts.
   */
  public void startSuperstep() throws IOException;

  /**
   * Called once completed a superstep.
   */
  public void finishSuperstep() throws IOException;

  /**
   * Must be called once a vertex is guaranteed not to change any more and can
   * safely be persisted to a secondary storage.
   */
  public void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException;

  /**
   * @return the number of vertices added to the underlying structure.
   *         Implementations should take care this is a constant time operation.
   */
  public int size();

  public IDSkippingIterator<V, E, M> skippingIterator();
}
