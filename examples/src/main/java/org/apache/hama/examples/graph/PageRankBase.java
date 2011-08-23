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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.examples.graph.partitioning.PartitionableWritable;
import org.apache.hama.examples.graph.partitioning.VertexPartitioner;

public abstract class PageRankBase extends BSP {
  public static final Log LOG = LogFactory.getLog(PageRankBase.class);

  protected static int MAX_ITERATIONS = 30;
  protected static final String MASTER_TASK = "master.groom";
  protected static double ALPHA;
  protected static int numOfVertices;
  protected static double DAMPING_FACTOR = 0.85;
  protected static double EPSILON = 0.001;

  private static final VertexPartitioner partitioner = new VertexPartitioner();

  static void mapAdjacencyList(Configuration conf, BSPPeer peer,
      HashMap<Vertex, List<Vertex>> realAdjacencyList,
      HashMap<Vertex, Double> tentativePagerank,
      HashMap<String, Vertex> lookupMap) throws FileNotFoundException,
      IOException {

    FileSystem fs = FileSystem.get(conf);
    Path p = new Path(conf.get("in.path." + peer.getPeerName().split(":")[0]));
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
    ObjectWritable key = new ObjectWritable(Vertex.class);
    key.setConf(conf);
    ArrayWritable value = new ArrayWritable(Vertex.class);
    while (reader.next(key, value)) {
      Vertex realKey = (Vertex) key.get();
      LinkedList<Vertex> list = new LinkedList<Vertex>();
      realAdjacencyList.put(realKey, list);
      lookupMap.put(realKey.name, realKey);
      tentativePagerank.put(realKey, Double.valueOf(1.0 / numOfVertices));
      for (Writable s : value.get()) {
        list.add((Vertex) s);
      }
    }

    reader.close();
  }

  static HamaConfiguration partitionTextFile(Path in, HamaConfiguration conf,
      String[] groomNames) throws IOException, InstantiationException,
      IllegalAccessException, InterruptedException {

    // set the partitioning vertex class
    conf.setClass("hama.partitioning.vertex.class", Vertex.class,
        PartitionableWritable.class);

    return (HamaConfiguration) partitioner.partition(conf, in, groomNames);
  }

  static HamaConfiguration partitionExample(Path out, HamaConfiguration conf,
      String[] groomNames) throws IOException, InstantiationException,
      IllegalAccessException, InterruptedException {

    /**
     * 1:twitter.com <br/>
     * 2:google.com <br/>
     * 3:facebook.com <br/>
     * 4:yahoo.com <br/>
     * 5:nasa.gov <br/>
     * 6:stackoverflow.com <br/>
     * 7:youtube.com
     */

    FileSystem fs = FileSystem.get(conf);
    Path input = new Path(out, "pagerank-example");
    FSDataOutputStream stream = fs.create(input);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream));

    String[] realNames = new String[] { null, "twitter.com", "google.com",
        "facebook.com", "yahoo.com", "nasa.gov", "stackoverflow.com",
        "youtube.com" };

    String[] lineArray = new String[] { "1;2;3", "2", "3;1;2;5", "4;5;6",
        "5;4;6", "6;4", "7;2;4" };

    for (String line : lineArray) {
      String[] arr = line.split(";");
      String vId = arr[0];
      int indexKey = Integer.valueOf(vId);

      String adjacents = "";
      for (int i = 1; i < arr.length; i++) {
        int index = Integer.valueOf(arr[i]);
        adjacents += realNames[index] + "\t";
      }
      writer.write(realNames[indexKey] + "\t" + adjacents);
      writer.newLine();
    }

    writer.close();
    fs.close();

    return partitionTextFile(input, conf, groomNames);
  }

  static void savePageRankMap(BSPPeer peer, Configuration conf,
      Map<Vertex, Double> tentativePagerank) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path outPath = new Path(conf.get("out.path") + Path.SEPARATOR + "temp"
        + Path.SEPARATOR
        + peer.getPeerName().split(ShortestPaths.NAME_VALUE_SEPARATOR)[0]);
    fs.delete(outPath, true);
    final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
        outPath, Text.class, DoubleWritable.class);
    for (Entry<Vertex, Double> row : tentativePagerank.entrySet()) {
      out.append(new Text(row.getKey().getName()),
          new DoubleWritable(row.getValue()));
    }
    out.close();
  }

  static void printOutput(FileSystem fs, Configuration conf) throws IOException {
    LOG.info("-------------------- RESULTS --------------------");
    FileStatus[] stati = fs.listStatus(new Path(conf.get("out.path")
        + Path.SEPARATOR + "temp"));
    for (FileStatus status : stati) {
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
