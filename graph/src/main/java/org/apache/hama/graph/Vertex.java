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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

public abstract class Vertex<M extends Writable> implements VertexInterface<M> {

  private M value;
  private String vertexID;
  protected GraphJobRunner runner;
  protected BSPPeer<?, ?, ?, ?, MapWritable> peer;
  public List<Edge> edges;

  public Configuration getConf() {
    return peer.getConfiguration();
  }

  @Override
  public String getVertexID() {
    return vertexID;
  }

  @Override
  public void setup(Configuration conf) {
  }

  @Override
  public void sendMessage(Edge e, M msg) throws IOException {
    MapWritable message = new MapWritable();
    message.put(new Text(e.getName()), msg);

    peer.send(e.getDestVertexID(), message);
  }

  @Override
  public void sendMessageToNeighbors(M msg) throws IOException {
    final List<Edge> outEdges = this.getOutEdges();
    for (Edge e : outEdges) {
      sendMessage(e, msg);
    }
  }

  @Override
  public long getSuperstepCount() {
    return runner.getNumberIterations();
  }

  @Override
  public List<Edge> getOutEdges() {
    return edges;
  }

  @Override
  public M getValue() {
    return value;
  }

  @Override
  public void setValue(M value) {
    this.value = value;
  }

  public void setVertexID(String vertexID) {
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
  public M getLastAggregatedValue() {
    return (M) runner.getLastAggregatedValue();
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

  public long getNumVertices() {
    return runner.getNumberVertices();
  }

  @Override
  public String toString() {
    return getVertexID() + (getValue() != null ? " = " + getValue() : "")
        + " // " + edges;
  }

}
