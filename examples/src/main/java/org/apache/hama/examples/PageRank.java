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
package org.apache.hama.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.graph.VertexWritable;
import org.apache.hama.util.KeyValuePair;

public class PageRank extends
    BSP<VertexWritable, ShortestPathVertexArrayWritable, Text, DoubleWritable> {
  public static final Log LOG = LogFactory.getLog(PageRank.class);

  private final HashMap<VertexWritable, VertexWritable[]> adjacencyList = new HashMap<VertexWritable, VertexWritable[]>();
  private final HashMap<String, VertexWritable> vertexLookupMap = new HashMap<String, VertexWritable>();
  private final HashMap<VertexWritable, Double> tentativePagerank = new HashMap<VertexWritable, Double>();
  // backup of the last pagerank to determine the error
  private final HashMap<VertexWritable, Double> lastTentativePagerank = new HashMap<VertexWritable, Double>();

  protected static int MAX_ITERATIONS = 30;
  protected static String masterTaskName;
  protected static double ALPHA;
  protected static int numOfVertices;
  protected static double DAMPING_FACTOR = 0.85;
  protected static double EPSILON = 0.001;

  @Override
  public void setup(
      BSPPeer<VertexWritable, ShortestPathVertexArrayWritable, Text, DoubleWritable> peer)
      throws IOException {
    // map our stuff into ram

    KeyValuePair<VertexWritable, ShortestPathVertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      adjacencyList.put(next.getKey(), (ShortestPathVertex[]) next.getValue()
          .toArray());
      vertexLookupMap.put(next.getKey().getName(), next.getKey());
    }

    // normally this is the global number of vertices
    numOfVertices = vertexLookupMap.size();
    DAMPING_FACTOR = Double.parseDouble(conf.get("damping.factor"));
    ALPHA = (1 - DAMPING_FACTOR) / (double) numOfVertices;
    EPSILON = Double.parseDouble(conf.get("epsilon.error"));
    MAX_ITERATIONS = Integer.parseInt(conf.get("max.iterations"));
    masterTaskName = peer.getPeerName(0);
  }

  @Override
  public void bsp(
      BSPPeer<VertexWritable, ShortestPathVertexArrayWritable, Text, DoubleWritable> peer)
      throws IOException, SyncException, InterruptedException {

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
        HashMap<VertexWritable, Double> sumMap = new HashMap<VertexWritable, Double>();
        DoubleMessage msg = null;
        while ((msg = (DoubleMessage) peer.getCurrentMessage()) != null) {
          VertexWritable k = vertexLookupMap.get(msg.getTag());
          if (k == null) {
            LOG.fatal("If you see this, partitioning has totally failed.");
          }
          if (!sumMap.containsKey(k)) {
            sumMap.put(k, msg.getData());
          } else {
            sumMap.put(k, msg.getData() + sumMap.get(k));
          }
        }
        // pregel formula:
        // ALPHA = 0.15 / NumVertices()
        // P(i) = ALPHA + 0.85 * sum
        for (Entry<VertexWritable, Double> entry : sumMap.entrySet()) {
          tentativePagerank.put(entry.getKey(), ALPHA
              + (entry.getValue() * DAMPING_FACTOR));
        }

        // determine the error and send this to the master
        double err = determineError();
        error = broadcastError(peer, err);
      }
      // in every step send the tentative pagerank of a vertex to its
      // adjacent vertices
      for (VertexWritable vertex : adjacencyList.keySet()) {
        sendMessageToNeighbors(peer, vertex);
      }

      iteration++;
    }

    // Clears all queues entries after we finished.
    peer.clear();
  }

  @Override
  public void cleanup(
      BSPPeer<VertexWritable, ShortestPathVertexArrayWritable, Text, DoubleWritable> peer) {
    try {
      for (Entry<VertexWritable, Double> row : tentativePagerank.entrySet()) {
        peer.write(new Text(row.getKey().getName()), new DoubleWritable(row
            .getValue()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private double broadcastError(
      BSPPeer<VertexWritable, ShortestPathVertexArrayWritable, Text, DoubleWritable> peer,
      double error) throws IOException, SyncException, InterruptedException {
    peer.send(masterTaskName, new DoubleMessage("", error));
    peer.sync();
    if (peer.getPeerName().equals(masterTaskName)) {
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
    for (Entry<VertexWritable, Double> entry : tentativePagerank.entrySet()) {
      error += Math.abs(lastTentativePagerank.get(entry.getKey())
          - entry.getValue());
    }
    return error;
  }

  private void copyTentativePageRankToBackup() {
    for (Entry<VertexWritable, Double> entry : tentativePagerank.entrySet()) {
      lastTentativePagerank.put(entry.getKey(), entry.getValue());
    }
  }

  private void sendMessageToNeighbors(
      BSPPeer<VertexWritable, ShortestPathVertexArrayWritable, Text, DoubleWritable> peer,
      VertexWritable v) throws IOException {
    VertexWritable[] outgoingEdges = adjacencyList.get(v);
    for (VertexWritable adjacent : outgoingEdges) {
      int mod = Math.abs(adjacent.hashCode() % peer.getNumPeers());
      // send a message of the tentative pagerank divided by the size of
      // the outgoing edges to all adjacents
      peer.send(peer.getPeerName(mod), new DoubleMessage(adjacent.getName(),
          tentativePagerank.get(v) / outgoingEdges.length));
    }
  }

  public static void printUsage() {
    System.out.println("PageRank Example:");
    System.out
        .println("<damping factor> <epsilon error> <optional: output path> <optional: input path>");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    if (args.length == 0) {
      printUsage();
      System.exit(-1);
    }

    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    BSPJob job = new BSPJob(conf);
    job.setOutputPath(new Path("pagerank/output"));

    // set the defaults
    conf.set("damping.factor", "0.85");
    conf.set("epsilon.error", "0.000001");

    boolean inputGiven = false;
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
        LOG.info("Set output path to " + args[2]);
        job.setOutputPath(new Path(args[2]));
        if (args.length == 4) {
          job.setInputPath(new Path(args[3]));
          LOG.info("Using custom input at " + args[3]);
          inputGiven = true;
        }
      }
    }

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    // leave the iterations on default
    conf.set("max.iterations", "0");

    if (!inputGiven) {
      Path tmp = new Path("pagerank/input");
      FileSystem.get(conf).delete(tmp, true);
      // ShortestPathsGraphLoader.loadGraph(conf, tmp);
      job.setInputPath(tmp);
    }

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setPartitioner(HashPartitioner.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumBspTask(cluster.getMaxTasks());
    job.setBspClass(PageRank.class);
    job.setJarByClass(PageRank.class);
    job.setJobName("Pagerank");
    if (job.waitForCompletion(true)) {
      printOutput(FileSystem.get(conf), conf);
    }
  }

  static void printOutput(FileSystem fs, Configuration conf) throws IOException {
    LOG.info("-------------------- RESULTS --------------------");
    FileStatus[] stati = fs.listStatus(new Path(conf.get("bsp.output.dir")));
    for (FileStatus status : stati) {
      if (!status.isDir() && !status.getPath().getName().endsWith(".crc")) {
        Path path = status.getPath();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        DoubleWritable value = new DoubleWritable();
        while (reader.next(key, value)) {
          LOG.info(key.toString() + " | " + value.get());
        }
        reader.close();
      }
    }
  }

}
