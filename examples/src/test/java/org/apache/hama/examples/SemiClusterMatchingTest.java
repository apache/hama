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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.message.compress.SnappyCompressor;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.ml.semiclustering.SemiClusterMessage;
import org.apache.hama.ml.semiclustering.SemiClusterTextReader;
import org.apache.hama.ml.semiclustering.SemiClusterVertexOutputWriter;
import org.apache.hama.ml.semiclustering.SemiClusteringVertex;
import org.junit.Test;

@SuppressWarnings("unused")
public class SemiClusterMatchingTest extends TestCase {
  private static String INPUT = "/tmp/graph.txt";
  private static String OUTPUT = "/tmp/graph-semiCluster";
  private static final String semiClusterMaximumVertexCount = "semicluster.max.vertex.count";
  private static final String graphJobMessageSentCount = "semicluster.max.message.sent.count";
  private static final String graphJobVertexMaxClusterCount = "vertex.max.cluster.count";
  private Configuration conf = new HamaConfiguration();
  private static FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  private void generateTestData() throws IOException {
    int vertexNameStart = 0, vertexNameEnd = 300, vertexEdgeMin = 30, vertexEdgeMax = 40;
    BufferedWriter bw = new BufferedWriter(new FileWriter(INPUT));
    for (int i = vertexNameStart; i < vertexNameEnd; i++) {
      StringBuilder sb = new StringBuilder();
      int numberOfEdges = 10;
      Map<Integer, Integer> mp = new HashMap<Integer, Integer>();
      int mapSize = 0, min = i - (i % 10), max = i + (10 - (i % 10) - 1);
      while (mapSize < numberOfEdges) {
        int edgeVal = randomInRange(min, max);
        if (mp.containsKey(edgeVal)) {
          int val = mp.get(edgeVal);
          mp.put(edgeVal, val++);
        } else
          mp.put(edgeVal, 1);
        mapSize = mp.size();
      }
      Iterator<Integer> itr = mp.keySet().iterator();
      sb.append(i + "\t");
      for (int j = 0; j < numberOfEdges; j++) {
        int key = itr.next();
        if (j != numberOfEdges - 1)
          sb.append(key + "-" + (float) mp.get(key) / (float) numberOfEdges
              + ",");
        else
          sb.append(key + "-" + (float) mp.get(key) / (float) numberOfEdges);
      }
      bw.write(sb.toString());
      if (i < vertexNameEnd)
        bw.write("\n");
    }
    bw.close();
  }

  public static int random(int max) {
    return (new Random().nextInt(max));
  }

  public static int randomInRange(int aStart, int aEnd) {
    Random random = new Random();
    if (aStart > aEnd) {
      throw new IllegalArgumentException("Start cannot exceed End.");
    }
    long range = (long) aEnd - (long) aStart + 1;
    long fraction = (long) (range * random.nextDouble());
    int randomNumber = (int) (fraction + aStart);
    return randomNumber;
  }

  public static Map<String, List<String>> inputGraphLoader() throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(INPUT));
    String line, firstVal;
    List<String> tm = new ArrayList<String>();
    Map<String, List<String>> mp = new HashMap<String, List<String>>();
    while ((line = br.readLine()) != null) {
      StringTokenizer st1 = new StringTokenizer(line, "\t");
      firstVal = st1.nextToken();
      StringTokenizer st2 = new StringTokenizer(st1.nextToken(), ",");
      while (st2.hasMoreTokens()) {
        tm.add(st2.nextToken().split("-")[0]);
      }
      mp.put(firstVal, tm);
    }
    br.close();
    return mp;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Map<String, List<String>> outputClusterLoader()
      throws IOException {
    FileStatus[] files = fs.globStatus(new Path(OUTPUT + "/part-*"));
    String line, vertexId, clusterId, clusterList;
    List<String> tm;
    Map<String, List<String>> mp = new HashMap<String, List<String>>();

    for (FileStatus file : files) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(file.getPath())));
      while ((line = reader.readLine()) != null) {
        StringTokenizer st1 = new StringTokenizer(line, "\t");
        vertexId = st1.nextToken();
        clusterList = st1.nextToken().toString().replaceAll("[\\[\\] ]", "");
        StringTokenizer st2 = new StringTokenizer(clusterList, ",");
        while (st2.hasMoreTokens()) {
          clusterId = st2.nextToken();
          if (!mp.containsKey(clusterId)) {
            tm = new ArrayList<String>();
            mp.put(clusterId, tm);
          } else
            tm = mp.get(clusterId);
          tm.add(vertexId);
          mp.put(clusterId, tm);
        }
      }
      reader.close();
    }

    Iterator it = mp.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pairs = (Map.Entry) it.next();
      List<String> ls = (List<String>) pairs.getValue();
      if (ls.size() == 1) {
        it.remove();
      }
    }
    return mp;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void semiClusterOutputChecker() throws IOException {
    int count = 0;
    boolean flag;
    Map<String, List<String>> mp = inputGraphLoader();
    Map<String, List<String>> mpOutPutCluser = outputClusterLoader();
    Iterator it = mpOutPutCluser.entrySet().iterator();
    while (it.hasNext()) {
      System.out.println(it.next());
      flag = true;
      Map.Entry pairs = (Map.Entry) it.next();
      List<String> valFromMap = new ArrayList<String>();
      List<String> val2 = (List<String>) pairs.getValue();
      int size = val2.size();
      for (int i = 0; i < size; i++) {
        valFromMap = mp.get(val2.get(0));
        val2.remove(0);
        if (!valFromMap.containsAll(val2)) {
          flag = false;
        }
      }
      if (flag == true) {
        count++;
      }
    }
    assertEquals("Semi Cluster Test Successful", 30, count);
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
      if (fs.exists(new Path("/tmp/partitions")))
        fs.delete(new Path("/tmp/partitions"), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSemiClustering() throws IOException, InterruptedException,
      ClassNotFoundException {
    generateTestData();
    try {

      HamaConfiguration conf = new HamaConfiguration();
      conf.setInt(semiClusterMaximumVertexCount, 100);
      conf.setInt(graphJobMessageSentCount, 100);
      conf.setInt(graphJobVertexMaxClusterCount, 1);
      GraphJob semiClusterJob = new GraphJob(conf, SemiClusterJobDriver.class);
      semiClusterJob.setMaxIteration(15);
      
      semiClusterJob.setCompressionCodec(SnappyCompressor.class);
      semiClusterJob.setCompressionThreshold(10);
      
      semiClusterJob
          .setVertexOutputWriterClass(SemiClusterVertexOutputWriter.class);
      semiClusterJob.setJobName("SemiClusterJob");
      semiClusterJob.setVertexClass(SemiClusteringVertex.class);
      semiClusterJob.setInputPath(new Path(INPUT));
      semiClusterJob.setOutputPath(new Path(OUTPUT));
      semiClusterJob.set("hama.graph.self.ref", "true");
      semiClusterJob.setVertexIDClass(Text.class);
      semiClusterJob.setVertexValueClass(SemiClusterMessage.class);
      semiClusterJob.setEdgeValueClass(DoubleWritable.class);
      semiClusterJob.setInputKeyClass(LongWritable.class);
      semiClusterJob.setInputValueClass(Text.class);
      semiClusterJob.setInputFormat(TextInputFormat.class);
      semiClusterJob.setVertexInputReaderClass(SemiClusterTextReader.class);
      semiClusterJob.setPartitioner(HashPartitioner.class);
      semiClusterJob.setOutputFormat(TextOutputFormat.class);
      semiClusterJob.setOutputKeyClass(Text.class);
      semiClusterJob.setOutputValueClass(Text.class);
      semiClusterJob.setNumBspTask(3);
      long startTime = System.currentTimeMillis();
      if (semiClusterJob.waitForCompletion(true)) {
        System.out.println("Job Finished in "
            + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
      }
      semiClusterOutputChecker();
    } finally {
      deleteTempDirs();
    }
  }

}
