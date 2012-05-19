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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

public abstract class Vertex<ID_TYPE extends Writable, MSG_TYPE extends Writable, EDGE_VALUE_TYPE extends Writable>
    implements VertexInterface<ID_TYPE, MSG_TYPE, EDGE_VALUE_TYPE> {

  private MSG_TYPE value;
  private ID_TYPE vertexID;
  protected GraphJobRunner<ID_TYPE, MSG_TYPE, EDGE_VALUE_TYPE> runner;
  protected BSPPeer<VertexWritable<ID_TYPE, MSG_TYPE>, VertexArrayWritable, Writable, Writable, GraphJobMessage> peer;
  public List<Edge<ID_TYPE, EDGE_VALUE_TYPE>> edges;

  public Configuration getConf() {
    return peer.getConfiguration();
  }

  @Override
  public ID_TYPE getVertexID() {
    return vertexID;
  }

  @Override
  public void setup(Configuration conf) {
  }

  @Override
  public void sendMessage(Edge<ID_TYPE, EDGE_VALUE_TYPE> e, MSG_TYPE msg)
      throws IOException {
    peer.send(e.getDestinationPeerName(),
        new GraphJobMessage(e.getDestinationVertexID(), msg));
  }

  @Override
  public void sendMessageToNeighbors(MSG_TYPE msg) throws IOException {
    final List<Edge<ID_TYPE, EDGE_VALUE_TYPE>> outEdges = this.getOutEdges();
    for (Edge<ID_TYPE, EDGE_VALUE_TYPE> e : outEdges) {
      sendMessage(e, msg);
    }
  }

  @Override
  public long getSuperstepCount() {
    return runner.getNumberIterations();
  }

  @Override
  public List<Edge<ID_TYPE, EDGE_VALUE_TYPE>> getOutEdges() {
    return edges;
  }

  @Override
  public MSG_TYPE getValue() {
    return value;
  }

  @Override
  public void setValue(MSG_TYPE value) {
    this.value = value;
  }

  public void setVertexID(ID_TYPE vertexID) {
    this.vertexID = vertexID;
  }

  public int getMaxIteration() {
    return runner.getMaxIteration();
  }

  /**
   * Get the last aggregated value of the defined aggregator, null if nothing
   * was configured or not returned a result.
   */
  @SuppressWarnings("unchecked")
  public MSG_TYPE getLastAggregatedValue() {
    return (MSG_TYPE) runner.getLastAggregatedValue();
  }

  /**
   * Get the number of aggregated vertices in the last superstep. Or null if no
   * aggregator is available.
   */
  public IntWritable getNumLastAggregatedVertices() {
    return runner.getNumLastAggregatedVertices();
  }

  public int getNumPeers() {
    return peer.getNumPeers();
  }

  @Override
  public long getNumVertices() {
    return runner.getNumberVertices();
  }

  @Override
  public String toString() {
    return getVertexID() + (getValue() != null ? " = " + getValue() : "")
        + " // " + edges;
  }

}
