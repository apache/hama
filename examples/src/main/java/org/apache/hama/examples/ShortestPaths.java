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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public class ShortestPaths extends
    BSP<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> {
  public static final Log LOG = LogFactory.getLog(ShortestPaths.class);

  public static final String START_VERTEX = "shortest.paths.start.vertex.name";
  private String FLAG_MESSAGE = "updatesFrom:";
  private final List<ShortestPathVertex> vertexLookup = new ArrayList<ShortestPathVertex>();
  private final HashMap<ShortestPathVertex, ShortestPathVertex[]> adjacencyList = new HashMap<ShortestPathVertex, ShortestPathVertex[]>();
  private String masterTask;

  @Override
  public void bsp(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer)
      throws IOException, SyncException, InterruptedException {
    boolean updated = true;
    while (updated) {
      int updatesMade = 0;
      int globalUpdateCounts = 0;
      ShortestPathVertexMessage msg = null;
      Deque<ShortestPathVertex> updatedQueue = new LinkedList<ShortestPathVertex>();
      while ((msg = (ShortestPathVertexMessage) peer.getCurrentMessage()) != null) {
        if (msg.getTag().getName().startsWith(FLAG_MESSAGE)) {
          if (msg.getData() == Integer.MIN_VALUE) {
            updated = false;
          } else {
            globalUpdateCounts += msg.getData();
          }
        } else {
          int index = Collections.binarySearch(vertexLookup, msg.getTag());
          ShortestPathVertex vertex = vertexLookup.get(index);

          // check if we need an distance update
          if (vertex.getCost() > msg.getData()) {
            updatesMade++;
            updatedQueue.add(vertex);
            vertex.setCost(msg.getData());
          }
        }
      }

      if (globalUpdateCounts == 0 && peer.getPeerName().equals(masterTask)
          && peer.getSuperstepCount() > 1) {
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, new ShortestPathVertexMessage(
              new ShortestPathVertex((int) peer.getSuperstepCount(),
                  FLAG_MESSAGE + peer.getPeerName()), Integer.MIN_VALUE));
        }
      }

      // send updates to the adjacents of the updated vertices
      peer.send(masterTask, new ShortestPathVertexMessage(
          new ShortestPathVertex((int) peer.getSuperstepCount(), FLAG_MESSAGE
              + peer.getPeerName()), updatesMade));

      for (ShortestPathVertex vertex : updatedQueue) {
        sendMessageToNeighbors(peer, vertex);
      }

      peer.sync();
    }
  }

  public void setup(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer)
      throws IOException, SyncException, InterruptedException {
    KeyValuePair<ShortestPathVertex, ShortestPathVertexArrayWritable> next = null;
    ShortestPathVertex startVertex = null;

    while ((next = peer.readNext()) != null) {
      if (next.getKey().getName().equals(
          peer.getConfiguration().get(START_VERTEX))) {
        next.getKey().setCost(0);
        startVertex = next.getKey();
      }
      adjacencyList.put(next.getKey(), (ShortestPathVertex[]) next.getValue()
          .toArray());
      vertexLookup.add(next.getKey());
    }

    Collections.sort(vertexLookup);
    masterTask = peer.getPeerName(0);

    // initial message bypass
    if (startVertex != null) {
      sendMessageToNeighbors(peer, startVertex);
    }
    peer.sync();
  }

  @Override
  public void cleanup(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer)
      throws IOException {
    // write our map into hdfs
    for (Entry<ShortestPathVertex, ShortestPathVertex[]> entry : adjacencyList
        .entrySet()) {
      int cost = entry.getKey().getCost();
      if (cost < Integer.MAX_VALUE) {
        peer.write(new Text(entry.getKey().getName()), new IntWritable(cost));
      }
    }
  }

  /**
   * This method takes advantage of our partitioning: it uses the vertexID
   * (simply hash of the name) to determine the host where the message belongs
   * to. <br/>
   * It sends the current cost to the adjacent vertex + the edge weight. If cost
   * will be infinity we just going to send infinity, because summing the weight
   * will cause an integer overflow resulting in negative cost.
   * 
   * @param peer The peer we got through the BSP method.
   * @param id The vertex to all adjacent vertices the new cost has to be send.
   * @throws IOException
   */
  private void sendMessageToNeighbors(
      BSPPeer<ShortestPathVertex, ShortestPathVertexArrayWritable, Text, IntWritable> peer,
      ShortestPathVertex id) throws IOException {
    ShortestPathVertex[] outgoingEdges = adjacencyList.get(id);

    for (ShortestPathVertex adjacent : outgoingEdges) {
      String target = peer.getPeerName(Math.abs((adjacent.hashCode() % peer
          .getAllPeerNames().length)));

      peer.send(target, new ShortestPathVertexMessage(adjacent,
          id.getCost() == Integer.MAX_VALUE ? id.getCost() : id.getCost()
              + adjacent.getWeight()));
    }
  }

  static void printOutput(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] stati = fs.listStatus(new Path(conf.get("bsp.output.dir")));
    for (FileStatus status : stati) {
      if (!status.isDir() && !status.getPath().getName().endsWith(".crc")) {
        Path path = status.getPath();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        IntWritable value = new IntWritable();
        int x = 0;
        while (reader.next(key, value)) {
          System.out.println(key.toString() + " | " + value.get());
          x++;
          if (x > 10) {
            System.out.println("...");
            break;
          }
        }
        reader.close();
      }
    }
  }

  public static void printUsage() {
    System.out.println("Usage: <startNode> <input path> <output path> [tasks]");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, InstantiationException,
      IllegalAccessException {

    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    BSPJob bsp = new BSPJob(conf, ShortestPaths.class);
    // Set the job name
    bsp.setJobName("Single Source Shortest Path");

    conf.set(START_VERTEX, args[0]);
    bsp.setInputPath(new Path(args[1]));
    bsp.setOutputPath(new Path(args[2]));

    if(args.length == 4) {
      bsp.setNumBspTask(Integer.parseInt(args[3]));
    }
    
    bsp.setBspClass(ShortestPaths.class);
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setPartitioner(HashPartitioner.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      printOutput(conf);
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
