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
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * VerticesInfo interface encapsulates the storage of vertices in a BSP Task.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
public interface IVerticesInfo<V extends WritableComparable<V>, E extends Writable, M extends Writable>
    extends Iterable<Vertex<V, E, M>> {

  /**
   * Add a vertex to the underlying structure.
   */
  public void addVertex(Vertex<V, E, M> vertex);

  /**
   * @return the number of vertices added to the underlying structure.
   *         Implementations should take care this is a constant time operation.
   */
  public int size();

  @Override
  public Iterator<Vertex<V, E, M>> iterator();

  // to be added and documented soon
  public void recoverState(DataInput in);

  public void saveState(DataOutput out);

}
