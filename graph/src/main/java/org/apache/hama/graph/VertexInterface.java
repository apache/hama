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
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;

/**
 * The vertex interface.
 * 
 * @param <V> this type must be writable and should also implement equals and
 *          hashcode.
 * @param <E> the type used for storing edge values, usually the value of an
 *          edge.
 * @param <M> the type used for messaging, usually the value of a vertex.
 */
@SuppressWarnings("rawtypes")
public interface VertexInterface<V extends WritableComparable, E extends Writable, M extends Writable>
    extends WritableComparable<VertexInterface<V, E, M>> {

  /**
   * This method is called once before the Vertex computation begins. Since the
   * Vertex object is serializable, variables in your Vertex program always
   * should be declared a s static.
   * 
   */
  public void setup(HamaConfiguration conf);

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
  public void compute(Iterable<M> messages) throws IOException;

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
   * Sends a message to add a new vertex through the partitioner to the
   * appropriate BSP peer
   */
  public void addVertex(V vertexID, List<Edge<V, E>> edges, M value)
      throws IOException;

  /**
   * Removes current Vertex from local peer.
   */
  public void remove() throws IOException;

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
  
   /**
    * Provides a value to the specified aggregator.
    *
    * @throws IOException
    *
    * @param name identifies a aggregator
    * @param value value to be aggregated
    */
   public void aggregate(int index, M value) throws IOException;

   /**
    * Returns the value of the specified aggregator.
    */
   public Writable getAggregatedValue(int index);

}
