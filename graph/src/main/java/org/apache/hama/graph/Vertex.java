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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

public abstract class Vertex<M extends Writable> implements VertexInterface<M> {
  private M value;
  private String vertexID;
  protected BSPPeer<?, ?, ?, ?, MapWritable> peer;
  public List<Edge> edges;
  private long numVertices;

  public Configuration getConf() {
    return peer.getConfiguration();
  }

  @Override
  public String getVertexID() {
    return vertexID;
  }

  @Override
  public void sendMessage(Edge e, M msg) throws IOException {
    MapWritable message = new MapWritable();
    message.put(new Text(e.getName()), msg);

    peer.send(e.getTarget(), message);
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
    return peer.getSuperstepCount();
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
    return peer.getConfiguration().getInt("hama.graph.max.iteration", 30);
  }

  public int getNumPeers() {
    return peer.getNumPeers();
  }

  public long getNumVertices() {
    return numVertices;
  }

  public void setNumVertices(long NumVertices) {
    this.numVertices = NumVertices;
  }

}
