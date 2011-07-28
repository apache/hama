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
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeerProtocol;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.zookeeper.KeeperException;

public class PageRank extends PageRankBase {
  public static final Log LOG = LogFactory.getLog(PageRank.class);

  private Configuration conf;

  private HashMap<PageRankVertex, List<PageRankVertex>> adjacencyList;
  private final HashMap<String, PageRankVertex> lookupMap = new HashMap<String, PageRankVertex>();
  private final HashMap<PageRankVertex, Double> tentativePagerank = new HashMap<PageRankVertex, Double>();
  // backup of the last pagerank to determine the error
  private final HashMap<PageRankVertex, Double> lastTentativePagerank = new HashMap<PageRankVertex, Double>();
  private String[] peerNames;

  @Override
  public void bsp(BSPPeerProtocol peer) throws IOException, KeeperException,
      InterruptedException {
    String master = conf.get(MASTER_TASK);
    // setup the datasets
    adjacencyList = PageRankBase.mapAdjacencyList(getConf(), peer);
    // init the pageranks to 1/n where n is the number of all vertices
    for (PageRankVertex vertex : adjacencyList.keySet()) {
      tentativePagerank.put(vertex, Double.valueOf(1.0 / numOfVertices));
      lookupMap.put(vertex.getUrl(), vertex);
    }
    // while the error not converges against epsilon do the pagerank stuff
    double error = 1.0;
    int iteration = 0;
    // if MAX_ITERATIONS are set to 0, ignore the iterations and just go
    // with the error
    while ((MAX_ITERATIONS > 0 && iteration < MAX_ITERATIONS)
        || error >= EPSILON) {
      peer.sync();

      if (iteration >= 1) {
        // copy the old pagerank to the backup
        copyTentativePageRankToBackup();
        // sum up all incoming messages for a vertex
        HashMap<PageRankVertex, Double> sumMap = new HashMap<PageRankVertex, Double>();
        DoubleMessage msg = null;
        while ((msg = (DoubleMessage) peer.getCurrentMessage()) != null) {
          PageRankVertex k = lookupMap.get(msg.getTag());
          if (!sumMap.containsKey(k)) {
            sumMap.put(k, msg.getData());
          } else {
            sumMap.put(k, msg.getData() + sumMap.get(k));
          }
        }
        // pregel formula:
        // ALPHA = 0.15 / NumVertices()
        // P(i) = ALPHA + 0.85 * sum
        for (Entry<PageRankVertex, Double> entry : sumMap.entrySet()) {
          tentativePagerank.put(entry.getKey(), ALPHA
              + (entry.getValue() * DAMPING_FACTOR));
        }

        // determine the error and send this to the master
        double err = determineError();
        error = broadcastError(peer, master, err);
      }
      // in every step send the tentative pagerank of a vertex to its
      // adjacent vertices
      for (PageRankVertex vertex : adjacencyList.keySet()) {
        sendMessageToNeighbors(peer, vertex);
      }

      iteration++;
    }

    // Clears all queues entries.
    peer.clear();
    // finally save the chunk of pageranks
    PageRankBase.savePageRankMap(peer, conf, lastTentativePagerank);
    LOG.info("Finished with iteration " + iteration + "!");
  }

  private double broadcastError(BSPPeerProtocol peer, String master,
      double error) throws IOException, KeeperException, InterruptedException {
    peer.send(master, new DoubleMessage("", error));
    peer.sync();
    if (peer.getPeerName().equals(master)) {
      double errorSum = 0.0;
      int count = 0;
      DoubleMessage message;
      while ((message = (DoubleMessage) peer.getCurrentMessage()) != null) {
        errorSum += message.getData();
        count++;
      }
      double avgError = errorSum / (double) count;
      // LOG.info("Average error: " + avgError);
      for (String name : peer.getAllPeerNames()) {
        peer.send(name, new DoubleMessage("", avgError));
      }
    }

    peer.sync();
    DoubleMessage message = (DoubleMessage) peer.getCurrentMessage();
    return message.getData();
  }

  private double determineError() {
    double error = 0.0;
    for (Entry<PageRankVertex, Double> entry : tentativePagerank.entrySet()) {
      error += Math.abs(lastTentativePagerank.get(entry.getKey())
          - entry.getValue());
    }
    return error;
  }

  private void copyTentativePageRankToBackup() {
    for (Entry<PageRankVertex, Double> entry : tentativePagerank.entrySet()) {
      lastTentativePagerank.put(entry.getKey(), entry.getValue());
    }
  }

  private void sendMessageToNeighbors(BSPPeerProtocol peer, PageRankVertex v)
      throws IOException {
    List<PageRankVertex> outgoingEdges = adjacencyList.get(v);
    for (PageRankVertex adjacent : outgoingEdges) {
      int mod = Math.abs(adjacent.getId() % peerNames.length);
      // send a message of the tentative pagerank divided by the size of
      // the outgoing edges to all adjacents
      peer.send(peerNames[mod], new DoubleMessage(adjacent.getUrl(),
          tentativePagerank.get(v) / outgoingEdges.size()));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    numOfVertices = Integer.parseInt(conf.get("num.vertices"));
    DAMPING_FACTOR = Double.parseDouble(conf.get("damping.factor"));
    ALPHA = (1 - DAMPING_FACTOR) / (double) numOfVertices;
    EPSILON = Double.parseDouble(conf.get("epsilon.error"));
    MAX_ITERATIONS = Integer.parseInt(conf.get("max.iterations"));
    peerNames = conf.get(ShortestPaths.BSP_PEERS).split(";");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void printUsage() {
    System.out.println("PageRank Example:");
    System.out
        .println("<damping factor> <epsilon error> <optional: output path> <optional: input path>");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length == 0) {
      printUsage();
      System.exit(-1);
    }

    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    // set the defaults
    conf.set("damping.factor", "0.85");
    conf.set("epsilon.error", "0.000001");

    if (args.length < 2) {
      System.out.println("You have to provide a damping factor and an error!");
      System.out.println("Try using 0.85 0.001 as parameter!");
      System.exit(-1);
    } else {
      conf.set("damping.factor", args[0]);
      conf.set("epsilon.error", args[1]);
      LOG.info("Set damping factor to " + args[0]);
      LOG.info("Set epsilon error to " + args[1]);
      if (args.length > 2) {
        conf.set("out.path", args[2]);
        LOG.info("Set output path to " + args[2]);
        if (args.length == 4) {
          conf.set("in.path", args[3]);
          LOG.info("Using custom input at " + args[3]);
        } else {
          LOG.info("Running default example graph!");
        }
      } else {
        conf.set("out.path", "pagerank/output");
        LOG.info("Set output path to default of pagerank/output!");
      }
    }

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);
    StringBuilder sb = new StringBuilder();
    for (String peerName : cluster.getActiveGroomNames().values()) {
      conf.set(MASTER_TASK, peerName);
      sb.append(peerName + ";");
    }

    // put every peer into the configuration
    conf.set(ShortestPaths.BSP_PEERS, sb.toString());
    // leave the iterations on default
    conf.set("max.iterations", "0");

    if (conf.get("in.path") == null) {
      conf = PageRankBase
          .partitionExample(new Path(conf.get("out.path")), conf);
    } else {
      conf = PageRankBase
          .partitionTextFile(new Path(conf.get("in.path")), conf);
    }

    BSPJob job = new BSPJob(conf);
    job.setNumBspTask(cluster.getGroomServers());
    job.setBspClass(PageRank.class);
    job.setJarByClass(PageRank.class);
    job.setJobName("Pagerank");
    if (job.waitForCompletion(true)) {
      PageRankBase.printOutput(FileSystem.get(conf), conf);
    }
  }
}
