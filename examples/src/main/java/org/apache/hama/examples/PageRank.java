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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;
import org.apache.hama.util.KeyValuePair;

public class PageRank extends
    BSP<VertexWritable, VertexArrayWritable, Text, DoubleWritable> {

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
      BSPPeer<VertexWritable, VertexArrayWritable, Text, DoubleWritable> peer)
      throws IOException {

    DAMPING_FACTOR = Double.parseDouble(conf.get("damping.factor"));
    EPSILON = Double.parseDouble(conf.get("epsilon.error"));
    MAX_ITERATIONS = Integer.parseInt(conf.get("max.iterations"));
    masterTaskName = peer.getPeerName(0);

    // map our stuff into ram
    KeyValuePair<VertexWritable, VertexArrayWritable> next = null;
    while ((next = peer.readNext()) != null) {
      adjacencyList.put(next.getKey(), (VertexWritable[]) next.getValue()
          .toArray());
      vertexLookupMap.put(next.getKey().getName(), next.getKey());
    }

    // normally this should be the global number of vertices
    numOfVertices = vertexLookupMap.size();
    ALPHA = (1 - DAMPING_FACTOR) / (double) numOfVertices;

    // reread the input to save ram
    peer.reopenInput();
    VertexWritable key = new VertexWritable();
    VertexArrayWritable value = new VertexArrayWritable();
    while (peer.readNext(key, value)) {
      VertexWritable vertexWritable = vertexLookupMap.get(key.getName());
      tentativePagerank
          .put(vertexWritable, Double.valueOf(1.0 / numOfVertices));
    }
  }

  @Override
  public void bsp(
      BSPPeer<VertexWritable, VertexArrayWritable, Text, DoubleWritable> peer)
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
      BSPPeer<VertexWritable, VertexArrayWritable, Text, DoubleWritable> peer) {
    try {
      for (Entry<VertexWritable, Double> row : tentativePagerank.entrySet()) {
        peer.write(new Text(row.getKey().getName()),
            new DoubleWritable(row.getValue()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private double broadcastError(
      BSPPeer<VertexWritable, VertexArrayWritable, Text, DoubleWritable> peer,
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
      BSPPeer<VertexWritable, VertexArrayWritable, Text, DoubleWritable> peer,
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

  static void printOutput(FileSystem fs, Configuration conf) throws IOException {
    LOG.info("-------------------- RESULTS --------------------");
    FileStatus[] stati = fs.listStatus(new Path(conf.get("bsp.output.dir")));
    for (FileStatus status : stati) {
      if (!status.isDir() && !status.getPath().getName().endsWith(".crc")) {
        Path path = status.getPath();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        DoubleWritable value = new DoubleWritable();
        int count = 0;
        while (reader.next(key, value)) {
          LOG.info(key.toString() + " | " + value.get());
          count++;
          if (count > 5)
            break;
        }
        reader.close();
      }
    }
  }

  public static void printUsage() {
    System.out.println("PageRank Example:");
    System.out
        .println("<input path> <output path> [damping factor] [epsilon error] [tasks]");

  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    if (args.length == 0) {
      printUsage();
      System.exit(-1);
    }

    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    BSPJob job = new BSPJob(conf, PageRank.class);
    job.setJobName("Pagerank");

    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));
    
    conf.set("damping.factor", (args.length > 2) ? args[2] : "0.85");
    conf.set("epsilon.error", (args.length > 3) ? args[3] : "0.000001");
    if (args.length == 5) {
      job.setNumBspTask(Integer.parseInt(args[4]));
    }

    // leave the iterations on default
    conf.set("max.iterations", "0");

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setPartitioner(HashPartitioner.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setBspClass(PageRank.class);

    long startTime = System.currentTimeMillis();
    if (job.waitForCompletion(true)) {
      printOutput(FileSystem.get(conf), conf);
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
