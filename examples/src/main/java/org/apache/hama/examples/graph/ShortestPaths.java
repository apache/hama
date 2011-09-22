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
package org.apache.hama.examples.graph;

import java.io.IOException;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BooleanMessage;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.IntegerMessage;
import org.apache.hama.examples.RandBench;
import org.apache.zookeeper.KeeperException;

public class ShortestPaths extends ShortestPathsBase {
  public static final Log LOG = LogFactory.getLog(ShortestPaths.class);

  private Configuration conf;
  private final HashMap<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList = new HashMap<ShortestPathVertex, List<ShortestPathVertex>>();
  private final HashMap<String, ShortestPathVertex> vertexLookupMap = new HashMap<String, ShortestPathVertex>();
  private String[] peerNames;

  @Override
  public void bsp(BSPPeer peer) throws IOException, KeeperException,
      InterruptedException {
    // map our input into ram
    mapAdjacencyList(conf, peer, adjacencyList, vertexLookupMap);
    // parse the configuration to get the peerNames
    parsePeerNames(conf);
    // get our master groom
    String master = conf.get(MASTER_TASK);

    // initial message bypass
    ShortestPathVertex v = vertexLookupMap.get(conf
        .get(SHORTEST_PATHS_START_VERTEX_ID));
    if (v != null) {
      v.setCost(0);
      sendMessageToNeighbors(peer, v);
    }

    boolean updated = true;
    while (updated) {
      int updatesMade = 0;
      peer.sync();

      IntegerMessage msg = null;
      Deque<ShortestPathVertex> updatedQueue = new LinkedList<ShortestPathVertex>();
      while ((msg = (IntegerMessage) peer.getCurrentMessage()) != null) {
        ShortestPathVertex vertex = vertexLookupMap.get(msg.getTag());
        // check if we need an distance update
        if (vertex.getCost() > msg.getData()) {
          updatesMade++;
          updatedQueue.add(vertex);
          vertex.setCost(msg.getData());
        }
      }
      // synchonize with all grooms if there were updates
      updated = broadcastUpdatesMade(peer, master, updatesMade);
      // send updates to the adjacents of the updated vertices
      for (ShortestPathVertex vertex : updatedQueue) {
        sendMessageToNeighbors(peer, vertex);
      }
    }
    // finished, finally save our map to DFS.
    saveVertexMap(conf, peer, adjacencyList);
  }

  /**
   * Parses the peer names to fix inconsistency in bsp peer names from context.
   * 
   * @param conf
   */
  private void parsePeerNames(Configuration conf) {
    peerNames = conf.get(BSP_PEERS).split(";");
  }

  /**
   * This method broadcasts to a master groom how many updates were made. He
   * simply sums them up and sends a message back to the grooms if sum is
   * greater than zero.
   * 
   * @param peer The peer we got through the BSP method.
   * @param master The assigned master groom name.
   * @param updates How many updates were made?
   * @return True if we need another iteration, False if no updates can be made
   *         anymore.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private boolean broadcastUpdatesMade(BSPPeer peer, String master, int updates)
      throws IOException, KeeperException, InterruptedException {
    peer.send(master, new IntegerMessage(peer.getPeerName(), updates));
    peer.sync();
    if (peer.getPeerName().equals(master)) {
      int count = 0;
      IntegerMessage message;
      while ((message = (IntegerMessage) peer.getCurrentMessage()) != null) {
        count += message.getData();
      }

      for (String name : peer.getAllPeerNames()) {
        peer.send(name, new BooleanMessage("", count > 0 ? true : false));
      }
    }

    peer.sync();
    BooleanMessage message = (BooleanMessage) peer.getCurrentMessage();
    return message.getData();
  }

  /**
   * This method takes advantage of our partitioning: it uses the vertexID
   * (simply hash of the name) to determine the host where the message belongs
   * to. <br/>
   * It sends the current cost to the adjacent vertex + the edge weight. If cost
   * will be infinity we just going to send infinity, because summing the weight
   * will cause an integer overflow resulting in negative weights.
   * 
   * @param peer The peer we got through the BSP method.
   * @param id The vertex to all adjacent vertices the new cost has to be send.
   * @throws IOException
   */
  private void sendMessageToNeighbors(BSPPeer peer, ShortestPathVertex id)
      throws IOException {
    List<ShortestPathVertex> outgoingEdges = adjacencyList.get(id);
    for (ShortestPathVertex adjacent : outgoingEdges) {
      int mod = Math.abs((adjacent.getId() % peer.getAllPeerNames().length));
      peer.send(peerNames[mod],
          new IntegerMessage(adjacent.getName(),
              id.getCost() == Integer.MAX_VALUE ? id.getCost() : id.getCost()
                  + adjacent.getWeight()));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void printUsage() {
    System.out.println("Single Source Shortest Path Example:");
    System.out
        .println("<Startvertex name> <optional: output path> <optional: path to own adjacency list textfile!>");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, InstantiationException,
      IllegalAccessException {

    printUsage();

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    conf.set(SHORTEST_PATHS_START_VERTEX_ID, "Frankfurt");
    System.out.println("Setting default start vertex to \"Frankfurt\"!");
    conf.set(OUT_PATH, "sssp/output");
    Path adjacencyListPath = null;

    if (args.length > 0) {
      conf.set(SHORTEST_PATHS_START_VERTEX_ID, args[0]);
      System.out.println("Setting start vertex to " + args[0] + "!");

      if (args.length > 1) {
        conf.set(OUT_PATH, args[1]);
        System.out.println("Using new output folder: " + args[1]);
      }

      if (args.length > 2) {
        adjacencyListPath = new Path(args[2]);
      }

    }

    Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList = null;
    if (adjacencyListPath == null)
      adjacencyList = ShortestPathsGraphLoader.loadGraph();

    BSPJob bsp = new BSPJob(conf, RandBench.class);
    // Set the job name
    bsp.setJobName("Single Source Shortest Path");
    bsp.setBspClass(ShortestPaths.class);

    // Set the task size as a number of GroomServer
    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    Collection<String> activeGrooms = cluster.getActiveGroomNames().keySet();
    String[] grooms = activeGrooms.toArray(new String[activeGrooms.size()]);

    LOG.info("Starting data partitioning...");
    if (adjacencyList == null) {
      conf = (HamaConfiguration) partition(conf, adjacencyListPath, grooms);
    } else {
      conf = (HamaConfiguration) partitionExample(conf, adjacencyList, grooms);
    }
    LOG.info("Finished!");

    bsp.setNumBspTask(cluster.getGroomServers());

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
      printOutput(FileSystem.get(conf), conf);
    }
  }

}
