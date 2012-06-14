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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * The vertex interface.
 * 
 * @param <V> this type must be writable and should also implement equals and
 *          hashcode.
 * @param <E> the type used for storing edge values, usually the value of an
 *          edge.
 * @param <M> the type used for messaging, usually the value of a vertex.
 */
public interface VertexInterface<V extends Writable, E extends Writable, M extends Writable> {

  /**
   * Used to setup a vertex.
   */
  public void setup(Configuration conf);

  /**
   * @return the unique identification for the vertex.
   */
  public V getVertexID();

  /**
   * @return the number of vertices in the input graph.
   */
  public long getNumVertices();

  /**
   * The user-defined function
   */
  public void compute(Iterator<M> messages) throws IOException;

  /**
   * @return a list of outgoing edges of this vertex in the input graph.
   */
  public List<Edge<V, E>> getEdges();

  /**
   * Sends a message to another vertex.
   */
  public void sendMessage(Edge<V, E> e, M msg) throws IOException;

  /**
   * Sends a message to neighbors
   */
  public void sendMessageToNeighbors(M msg) throws IOException;

  /**
   * Sends a message to the given destination vertex by ID and the message value
   */
  public void sendMessage(V destinationVertexID, M msg) throws IOException;

  /**
   * @return the superstep number of the current superstep (starting from 0).
   */
  public long getSuperstepCount();

  /**
   * Vote to halt.
   */
  public void voteToHalt();

  /**
   * Sets the vertex value
   */
  public void setValue(M value);

  /**
   * Gets the vertex value
   */
  public M getValue();

}
