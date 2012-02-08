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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

@SuppressWarnings("rawtypes")
public class GraphJobRunner extends BSP {
  private Map<String, Vertex> vertices = new HashMap<String, Vertex>();

  @SuppressWarnings("unchecked")
  @Override
  public void bsp(BSPPeer peer) throws IOException, SyncException,
      InterruptedException {
    int maxIteration = peer.getConfiguration().getInt(
        "hama.graph.max.iteration", 30);

    boolean updated = true;
    int iteration = 0;
    while (updated && iteration < maxIteration) {
      peer.sync();

      BSPMessage msg = null;
      Map<String, LinkedList<BSPMessage>> msgMap = new HashMap<String, LinkedList<BSPMessage>>();
      while ((msg = peer.getCurrentMessage()) != null) {

        if (msgMap.containsKey(msg.getTag())) {
          LinkedList<BSPMessage> msgs = msgMap.get(msg.getTag());
          msgs.add(msg);
          msgMap.put((String) msg.getTag(), msgs);
        } else {
          LinkedList<BSPMessage> msgs = new LinkedList<BSPMessage>();
          msgs.add(msg);
          msgMap.put((String) msg.getTag(), msgs);
        }
      }

      if (msgMap.size() < 1) {
        updated = false;
      }

      for (Map.Entry<String, LinkedList<BSPMessage>> e : msgMap.entrySet()) {
        vertices.get(e.getKey()).compute(e.getValue().iterator());
      }
      iteration++;
    }
  }

  @SuppressWarnings("unchecked")
  public void setup(BSPPeer peer) throws IOException, SyncException,
      InterruptedException {
    KeyValuePair<? extends VertexWritable, ? extends VertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      Vertex vertex = (Vertex) ReflectionUtils.newInstance(
          peer.getConfiguration().getClass("hama.graph.vertex.class",
              Vertex.class), peer.getConfiguration());
      vertex.setVertexID(next.getKey().getName());
      vertex.peer = peer;

      VertexWritable[] arr = (VertexWritable[]) next.getValue().toArray();
      List<Edge> edges = new ArrayList<Edge>();
      for (VertexWritable e : arr) {
        String target = peer.getPeerName(Math.abs((e.hashCode() % peer
            .getAllPeerNames().length)));
        edges.add(new Edge(e.getName(), target, e.getWeight()));
      }

      vertex.edges = edges;
      vertices.put(next.getKey().getName(), vertex);
    }

    long numberVertices = vertices.size() * peer.getNumPeers();

    for (Map.Entry<String, Vertex> e : vertices.entrySet()) {
      LinkedList<BSPMessage> msgIterator = new LinkedList<BSPMessage>();

      try {
        BSPMessage msg = (BSPMessage) e.getValue().messageClass.newInstance();
        msg.setTag(e.getValue().getVertexID());
        msg.setData(e.getValue().getValue());
        msgIterator.add(msg);
      } catch (Exception e1) {
        // TODO init failed.
        e1.printStackTrace();
      }

      e.getValue().setNumVertices(numberVertices);
      e.getValue().compute(msgIterator.iterator());
    }
  }

  public void cleanup(BSPPeer peer) {
    // FIXME provide write solution to Vertex
    System.out.println("for debug\n==================");
    for (Map.Entry<String, Vertex> e : vertices.entrySet()) {
      System.out.println(e.getValue().getVertexID() + ", "
          + e.getValue().getValue());
    }
  }
}
