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
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

@SuppressWarnings("unchecked")
public class GraphJobRunner extends BSP {
  public static final Log LOG = LogFactory.getLog(GraphJobRunner.class);
  private Configuration conf;
  private Combiner<? extends Writable> combiner;
  private Map<String, Vertex> vertices = new HashMap<String, Vertex>();

  private String FLAG_MESSAGE = "hama.graph.msg.counts";
  private final String MESSAGE_COMBINER_CLASS = "hama.vertex.message.combiner.class";

  private String masterTask;
  private boolean updated = true;
  private int globalUpdateCounts = 0;

  @Override
  public void bsp(BSPPeer peer) throws IOException, SyncException,
      InterruptedException {
    int maxIteration = peer.getConfiguration().getInt(
        "hama.graph.max.iteration", 30);

    int iteration = 0;
    while (updated && iteration < maxIteration) {
      globalUpdateCounts = 0;
      peer.sync();

      // Map <vertexID, messages>
      Map<String, LinkedList<Writable>> messages = parseMessages(peer);

      // exit if there's no update made
      if (globalUpdateCounts == 0 && peer.getPeerName().equals(masterTask)
          && peer.getSuperstepCount() > 1) {
        MapWritable updatedCnt = new MapWritable();
        updatedCnt.put(new Text(FLAG_MESSAGE), new IntWritable(
            Integer.MIN_VALUE));

        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, updatedCnt);
        }
      }

      // send msgCounts to the master task
      MapWritable updatedCnt = new MapWritable();
      updatedCnt.put(new Text(FLAG_MESSAGE), new IntWritable(messages.size()));
      peer.send(masterTask, updatedCnt);

      for (Map.Entry<String, LinkedList<Writable>> e : messages.entrySet()) {
        LinkedList msgs = e.getValue();
        if (combiner != null) {
          Writable combined = combiner.combine(msgs);
          msgs = new LinkedList();
          msgs.add(combined);
        }

        vertices.get(e.getKey()).compute(msgs.iterator());
      }
      iteration++;
    }
  }

  private Map<String, LinkedList<Writable>> parseMessages(BSPPeer peer)
      throws IOException {
    MapWritable msg = null;
    Map<String, LinkedList<Writable>> msgMap = new HashMap<String, LinkedList<Writable>>();
    while ((msg = (MapWritable) peer.getCurrentMessage()) != null) {

      for (Entry<Writable, Writable> e : msg.entrySet()) {
        String vertexID = ((Text) e.getKey()).toString();

        if (vertexID.toString().equals(FLAG_MESSAGE)) {
          if (((IntWritable) e.getValue()).get() == Integer.MIN_VALUE) {
            updated = false;
          } else {
            globalUpdateCounts += ((IntWritable) e.getValue()).get();
          }
        } else {
          Writable value = e.getValue();

          if (msgMap.containsKey(vertexID)) {
            LinkedList<Writable> msgs = msgMap.get(vertexID);
            msgs.add(value);
            msgMap.put(vertexID, msgs);
          } else {
            LinkedList<Writable> msgs = new LinkedList<Writable>();
            msgs.add(value);
            msgMap.put(vertexID, msgs);
          }
        }
      }
    }

    return msgMap;
  }

  public void setup(BSPPeer peer) throws IOException, SyncException,
      InterruptedException {
    this.conf = peer.getConfiguration();
    // Choose one as a master to collect global updates
    this.masterTask = peer.getPeerName(0);

    if (!conf.getClass(MESSAGE_COMBINER_CLASS, Combiner.class).equals(
        Combiner.class)) {
      LOG.debug("Combiner class: " + conf.get(MESSAGE_COMBINER_CLASS));

      combiner = (Combiner<? extends Writable>) ReflectionUtils.newInstance(
          conf.getClass("hama.vertex.message.combiner.class", Combiner.class),
          conf);
    }

    loadVertices(peer);
    long numberVertices = vertices.size() * peer.getNumPeers();

    for (Map.Entry<String, Vertex> e : vertices.entrySet()) {
      e.getValue().setNumVertices(numberVertices);

      LinkedList<Writable> msgIterator = new LinkedList<Writable>();
      msgIterator.add(e.getValue().getValue());
      e.getValue().compute(msgIterator.iterator());
    }
  }

  private void loadVertices(BSPPeer peer) throws IOException {
    LOG.debug("vertex class: " + conf.get("hama.graph.vertex.class"));
    KeyValuePair<? extends VertexWritable, ? extends VertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      Vertex<? extends Writable> vertex = (Vertex<? extends Writable>) ReflectionUtils
          .newInstance(conf.getClass("hama.graph.vertex.class", Vertex.class),
              conf);
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
  }

  /**
   * Just write <new Text(vertexID), (Writable) value> pair as a result
   */
  public void cleanup(BSPPeer peer) throws IOException {
    for (Map.Entry<String, Vertex> e : vertices.entrySet()) {
      peer.write(new Text(e.getValue().getVertexID()), e.getValue().getValue());
      LOG.debug(e.getValue().getVertexID() + ", " + e.getValue().getValue());
    }
  }
}
