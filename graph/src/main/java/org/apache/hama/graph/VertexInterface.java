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
 * @param <ID_TYPE> this type must be writable and should also implement equals
 *          and hashcode.
 * @param <MSG_TYPE> the type used for messaging, usually the value of a vertex.
 * @param <EDGE_VALUE_TYPE> the type used for storing edge values, usually the
 *          value of an edge.
 */
public interface VertexInterface<ID_TYPE extends Writable, MSG_TYPE extends Writable, EDGE_VALUE_TYPE extends Writable> {

  /**
   * Used to setup a vertex.
   */
  public void setup(Configuration conf);

  /** @return the unique identification for the vertex. */
  public ID_TYPE getVertexID();

  /** @return the number of vertices in the input graph. */
  public long getNumVertices();

  /** The user-defined function */
  public void compute(Iterator<MSG_TYPE> messages) throws IOException;

  /** @return a list of outgoing edges of this vertex in the input graph. */
  public List<Edge<ID_TYPE, EDGE_VALUE_TYPE>> getOutEdges();

  /** Sends a message to another vertex. */
  public void sendMessage(Edge<ID_TYPE, EDGE_VALUE_TYPE> e, MSG_TYPE msg)
      throws IOException;

  /** Sends a message to neighbors */
  public void sendMessageToNeighbors(MSG_TYPE msg) throws IOException;

  /** @return the superstep number of the current superstep (starting from 0). */
  public long getSuperstepCount();

  /**
   * Sets the vertex value
   * 
   * @param value
   */
  public void setValue(MSG_TYPE value);

  /**
   * Gets the vertex value
   * 
   * @return value
   */
  public MSG_TYPE getValue();

}
