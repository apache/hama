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
package org.apache.hama.examples.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.examples.CombineExample;
import org.apache.hama.examples.PageRank.PageRankVertex;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.util.ReflectionUtils;

public class VertexInputGen {

  public static final String SIZE_OF_MATRIX = "size.of.matrix";
  public static final String DENSITY = "density.of.matrix";

  public static interface VertexCreator {
    @SuppressWarnings("rawtypes")
    Vertex createVertex(Text id, Text[] edges, Text value);
  }

  public static class PageRankVertexCreatorImpl implements VertexCreator {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Vertex createVertex(Text id, Text[] edges, Text value) {
      Vertex v = new PageRankVertex();
      v.setVertexID(id);
      for (Text t : edges) {
        v.addEdge(new Edge<Text, NullWritable>(t, null));
      }
      return v;
    }

  }

  public static int getVertexCaseId(Class<? extends Vertex> classObj) {
    if (classObj.getCanonicalName().equals(
        PageRankVertexCreatorImpl.class.getCanonicalName())) {
      return 1;
    }

    return -1;
  }

  @SuppressWarnings("rawtypes")
  public static class VertexInputGenBSP extends
      BSP<NullWritable, NullWritable, Vertex, NullWritable, Text> {

    private Configuration conf;
    private int sizeN;
    private int density;
    private Map<Integer, HashSet<Integer>> list = new HashMap<Integer, HashSet<Integer>>();
    private VertexCreator vertexCreator;

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, Vertex, NullWritable, Text> peer) {
      this.conf = peer.getConfiguration();
      sizeN = conf.getInt(SIZE_OF_MATRIX, 10);
      density = conf.getInt(DENSITY, 1);

      int vertexCase = conf.getInt("hama.test.vertexcreatorid", -1);
      if (vertexCase == 1) {
        vertexCreator = new PageRankVertexCreatorImpl();
      } else {
        throw new RuntimeException("No vertex creator specified");
      }

    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, Vertex, NullWritable, Text> peer)
        throws IOException, SyncException, InterruptedException {
      int interval = sizeN / peer.getNumPeers();
      int startID = peer.getPeerIndex() * interval;
      int endID;
      if (peer.getPeerIndex() == peer.getNumPeers() - 1)
        endID = sizeN;
      else
        endID = startID + interval;

      // Generate N*(N+1) elements for lower triangular
      for (int i = startID; i < endID; i++) {
        HashSet<Integer> edges = new HashSet<Integer>();
        for (int j = 0; j <= i; j++) {
          boolean nonZero = new Random().nextInt(density) == 0;
          if (nonZero && !edges.contains(j) && i != j) {
            edges.add(j);

            // TODO please refactor this.
            int peerIndex = j / interval;
            if (peerIndex == peer.getNumPeers())
              peerIndex = peerIndex - 1;

            peer.send(peer.getPeerName(j / interval), new Text(j + "," + i));
          }
        }

        list.put(i, edges);
      }

      // Synchronize the upper and lower
      peer.sync();
      Text received;
      while ((received = peer.getCurrentMessage()) != null) {
        String[] kv = received.toString().split(",");
        HashSet<Integer> nList = list.get(Integer.parseInt(kv[0]));
        nList.add(Integer.parseInt(kv[1]));
        list.put(Integer.parseInt(kv[0]), nList);
      }
    }

    @Override
    public void cleanup(
        BSPPeer<NullWritable, NullWritable, Vertex, NullWritable, Text> peer)
        throws IOException {
      for (Map.Entry<Integer, HashSet<Integer>> e : list.entrySet()) {
        Text[] values = new Text[e.getValue().size()];
        if (values.length > 0) {
          int i = 0;
          for (Integer v : e.getValue()) {
            values[i] = new Text(String.valueOf(v));
            i++;
          }
          peer.write(
              (Vertex)this.vertexCreator.createVertex(
                  new Text(String.valueOf(e.getKey())), values, new Text()),
              NullWritable.get());
        }
      }
    }
  }

  public static void runJob(HamaConfiguration conf, int numTasks, String output, Class<? extends Vertex> cls)
      throws IOException, InterruptedException, ClassNotFoundException {
    BSPJob bsp = new BSPJob(conf, VertexInputGen.class);
    // Set the job name
    bsp.setJobName("Random Vertex Input Generator");
    bsp.setBspClass(VertexInputGenBSP.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(cls);
    bsp.setOutputValueClass(NullWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, new Path(output));
    bsp.setNumBspTask(numTasks);

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    if (args.length < 4) {
      System.out
          .println("Usage: <size n> <1/x density> <output path> <number of tasks>");
      System.exit(1);
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    conf.setInt(SIZE_OF_MATRIX, Integer.parseInt(args[0]));
    conf.setInt(DENSITY, Integer.parseInt(args[1]));
    runJob(conf, Integer.parseInt(args[3]), args[2], Vertex.class);

  }

}
