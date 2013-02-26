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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;

/**
 * Vertex is a abstract definition of Google Pregel Vertex. For implementing a
 * graph application, one must implement a sub-class of Vertex and define, the
 * message passing and message processing for each vertex.
 * 
 * Every vertex should be assigned an ID. This ID object should obey the
 * equals-hashcode contract and would be used for partitioning.
 * 
 * The edges for a vertex could be accessed and modified using the
 * {@link Vertex#getEdges()} call. The self value of the vertex could be changed
 * by {@link Vertex#setValue(Writable)}.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<V extends WritableComparable, E extends Writable, M extends Writable>
    implements VertexInterface<V, E, M> {

  GraphJobRunner<?, ?, ?> runner;

  private V vertexID;
  private M value;
  private List<Edge<V, E>> edges;

  private boolean votedToHalt = false;

  public Configuration getConf() {
    return runner.getPeer().getConfiguration();
  }

  @Override
  public V getVertexID() {
    return vertexID;
  }

  @Override
  public void setup(Configuration conf) {
  }

  @Override
  public void sendMessage(Edge<V, E> e, M msg) throws IOException {
    runner.getPeer().send(getDestinationPeerName(e),
        new GraphJobMessage(e.getDestinationVertexID(), msg));
  }

  /**
   * @return the destination peer name of the destination of the given directed
   *         edge.
   */
  public String getDestinationPeerName(Edge<V, E> edge) {
    return getDestinationPeerName(edge.getDestinationVertexID());
  }

  /**
   * @return the destination peer name of the given vertex id, determined by the
   *         partitioner.
   */
  public String getDestinationPeerName(V vertexId) {
    return runner.getPeer().getPeerName(
        getPartitioner().getPartition(vertexId, value,
            runner.getPeer().getNumPeers()));
  }

  @Override
  public void sendMessageToNeighbors(M msg) throws IOException {
    final List<Edge<V, E>> outEdges = this.getEdges();
    for (Edge<V, E> e : outEdges) {
      sendMessage(e, msg);
    }
  }

  @Override
  public void sendMessage(V destinationVertexID, M msg) throws IOException {
    int partition = getPartitioner().getPartition(destinationVertexID, msg,
        runner.getPeer().getNumPeers());
    String destPeer = runner.getPeer().getAllPeerNames()[partition];
    runner.getPeer().send(destPeer,
        new GraphJobMessage(destinationVertexID, msg));
  }

  @Override
  public long getSuperstepCount() {
    return runner.getNumberIterations();
  }

  public void setEdges(List<Edge<V, E>> list) {
    this.edges = list;
  }

  public void addEdge(Edge<V, E> edge) {
    if (edges == null) {
      this.edges = new ArrayList<Edge<V, E>>();
    }
    this.edges.add(edge);
  }

  @Override
  public List<Edge<V, E>> getEdges() {
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

  public void setVertexID(V vertexID) {
    this.vertexID = vertexID;
  }

  public int getMaxIteration() {
    return runner.getMaxIteration();
  }

  /**
   * Get the last aggregated value of the defined aggregator, null if nothing
   * was configured or not returned a result. You have to supply an index, the
   * index is defined by the order you set the aggregator classes in
   * {@link GraphJob#setAggregatorClass(Class...)}. Index is starting at zero,
   * so if you have a single aggregator you can retrieve it via
   * {@link #getLastAggregatedValue}(0).
   */
  @SuppressWarnings("unchecked")
  public M getLastAggregatedValue(int index) {
    return (M) runner.getLastAggregatedValue(index);
  }

  /**
   * Get the number of aggregated vertices in the last superstep. Or null if no
   * aggregator is available.You have to supply an index, the index is defined
   * by the order you set the aggregator classes in
   * {@link GraphJob#setAggregatorClass(Class...)}. Index is starting at zero,
   * so if you have a single aggregator you can retrieve it via
   * {@link #getNumLastAggregatedVertices}(0).
   */
  public IntWritable getNumLastAggregatedVertices(int index) {
    return runner.getNumLastAggregatedVertices(index);
  }

  public int getNumPeers() {
    return runner.getPeer().getNumPeers();
  }

  /**
   * Gives access to the BSP primitives and additional features by a peer.
   */
  public BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> getPeer() {
    return runner.getPeer();
  }

  /**
   * @return the configured partitioner instance to message vertices.
   */
  @SuppressWarnings("unchecked")
  public Partitioner<V, M> getPartitioner() {
    return (Partitioner<V, M>) runner.getPartitioner();
  }

  @Override
  public long getNumVertices() {
    return runner.getNumberVertices();
  }

  @Override
  public void voteToHalt() {
    this.votedToHalt = true;
  }

  void setActive() {
    this.votedToHalt = false;
  }

  public boolean isHalted() {
    return votedToHalt;
  }

  void setVotedToHalt(boolean votedToHalt) {
    this.votedToHalt = votedToHalt;
  }

  @Override
  public int hashCode() {
    return ((vertexID == null) ? 0 : vertexID.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Vertex<?, ?, ?> other = (Vertex<?, ?, ?>) obj;
    if (vertexID == null) {
      if (other.vertexID != null)
        return false;
    } else if (!vertexID.equals(other.vertexID))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Active: " + !votedToHalt + " -> ID: " + getVertexID()
        + (getValue() != null ? " = " + getValue() : "") + " // " + edges;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      if (vertexID == null) {
        vertexID = GraphJobRunner.createVertexIDObject();
      }
      vertexID.readFields(in);
    }
    if (in.readBoolean()) {
      if (this.value == null) {
        value = GraphJobRunner.createVertexValue();
      }
      value.readFields(in);
    }
    this.edges = new ArrayList<Edge<V, E>>();
    if (in.readBoolean()) {
      int num = in.readInt();
      if (num > 0) {
        for (int i = 0; i < num; ++i) {
          V vertex = GraphJobRunner.createVertexIDObject();
          vertex.readFields(in);
          E edgeCost = null;
          if (in.readBoolean()) {
            edgeCost = GraphJobRunner.createEdgeCostObject();
            edgeCost.readFields(in);
          }
          Edge<V, E> edge = new Edge<V, E>(vertex, edgeCost);
          this.edges.add(edge);
        }

      }
    }
    votedToHalt = in.readBoolean();
    readState(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (vertexID == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      vertexID.write(out);
    }
    if (value == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      value.write(out);
    }
    if (this.edges == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(this.edges.size());
      for (Edge<V, E> edge : this.edges) {
        edge.getDestinationVertexID().write(out);
        if (edge.getValue() == null) {
          out.writeBoolean(false);
        } else {
          out.writeBoolean(true);
          edge.getValue().write(out);
        }
      }
    }
    out.writeBoolean(votedToHalt);
    writeState(out);

  }

  // compare across the vertex ID
  @SuppressWarnings("unchecked")
  @Override
  public final int compareTo(VertexInterface<V, E, M> o) {
    return getVertexID().compareTo(o.getVertexID());
  }

  /**
   * Read the state of the vertex from the input stream. The framework would
   * have already constructed and loaded the vertex-id, edges and voteToHalt
   * state. This function is essential if there is any more properties of vertex
   * to be read from.
   */
  public void readState(DataInput in) throws IOException {

  }

  /**
   * Writes the state of vertex to the output stream. The framework writes the
   * vertex and edge information to the output stream. This function could be
   * used to save the state variable of the vertex added in the implementation
   * of object.
   */
  public void writeState(DataOutput out) throws IOException {

  }

}
