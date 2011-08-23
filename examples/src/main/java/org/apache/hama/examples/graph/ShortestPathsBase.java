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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.examples.graph.partitioning.PartitionableWritable;
import org.apache.hama.examples.graph.partitioning.ShortestPathVertexPartitioner;

public abstract class ShortestPathsBase extends BSP {

  private static final String SSSP_EXAMPLE_FILE_NAME = "sssp-example";
  public static final String BSP_PEERS = "bsp.peers";
  public static final String SHORTEST_PATHS_START_VERTEX_ID = "shortest.paths.start.vertex.id";
  public static final String PARTED = "parted";
  public static final String IN_PATH = "in.path.";
  public static final String OUT_PATH = "out.path";
  public static final String NAME_VALUE_SEPARATOR = ":";
  public static final String MASTER_TASK = "master.groom";

  private static final ShortestPathVertexPartitioner partitioner = new ShortestPathVertexPartitioner();

  /**
   * When finished we just writing a sequencefile of the vertex name and the
   * cost.
   * 
   * @param peer The peer we got through the BSP method.
   * @param adjacencyList
   * @throws IOException
   */
  protected final static void saveVertexMap(Configuration conf, BSPPeer peer,
      Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList)
      throws IOException {
    Path outPath = new Path(conf.get(OUT_PATH) + Path.SEPARATOR
        + peer.getPeerName().split(":")[0]);
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outPath, true);
    final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
        outPath, Text.class, IntWritable.class);
    for (ShortestPathVertex vertex : adjacencyList.keySet()) {
      out.append(new Text(vertex.getName()), new IntWritable(vertex.getCost()));
    }
    out.close();
  }

  /**
   * Just a reader of the vertexMap in DFS. Output going to STDOUT.
   * 
   * @param fs
   * @param conf
   * @throws IOException
   */
  protected final static void printOutput(FileSystem fs, Configuration conf)
      throws IOException {
    System.out.println("-------------------- RESULTS --------------------");
    FileStatus[] stati = fs.listStatus(new Path(conf.get(OUT_PATH)));
    for (FileStatus status : stati) {
      if (!status.isDir() && !status.getPath().getName().equals(SSSP_EXAMPLE_FILE_NAME)) {
        Path path = status.getPath();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        IntWritable value = new IntWritable();
        while (reader.next(key, value)) {
          System.out.println(key.toString() + " | " + value.get());
        }
        reader.close();
      }
    }
  }

  protected final static void mapAdjacencyList(Configuration conf,
      BSPPeer peer,
      Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList,
      Map<String, ShortestPathVertex> vertexLookupMap)
      throws FileNotFoundException, IOException {

    FileSystem fs = FileSystem.get(conf);
    Path p = new Path(conf.get("in.path." + peer.getPeerName().split(":")[0]));
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
    ObjectWritable key = new ObjectWritable(ShortestPathVertex.class);
    key.setConf(conf);
    ArrayWritable value = new ArrayWritable(ShortestPathVertex.class);
    while (reader.next(key, value)) {
      ShortestPathVertex realKey = (ShortestPathVertex) key.get();
      realKey.setCost(Integer.MAX_VALUE);
      LinkedList<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
      adjacencyList.put(realKey, list);
      vertexLookupMap.put(realKey.name, realKey);

      for (Writable s : value.get()) {
        final ShortestPathVertex vertex = (ShortestPathVertex) s;
        vertex.setCost(Integer.MAX_VALUE);
        list.add(vertex);
      }
    }

    reader.close();
  }

  protected final static Configuration partitionExample(Configuration conf,
      Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList,
      String[] groomNames) throws IOException, InstantiationException,
      IllegalAccessException, InterruptedException {

    FileSystem fs = FileSystem.get(conf);
    Path input = new Path(conf.get(OUT_PATH), SSSP_EXAMPLE_FILE_NAME);
    FSDataOutputStream stream = fs.create(input);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream));

    Set<Entry<ShortestPathVertex, List<ShortestPathVertex>>> set = adjacencyList
        .entrySet();

    for (Entry<ShortestPathVertex, List<ShortestPathVertex>> entry : set) {

      String line = entry.getKey().getName();

      for (ShortestPathVertex v : entry.getValue()) {
        line += "\t" + v.getName() + ":" + v.getWeight();
      }

      writer.write(line);
      writer.newLine();

    }

    writer.close();
    fs.close();

    return partition(conf, input, groomNames);
  }

  protected final static Configuration partition(Configuration conf,
      Path fileToPartition, String[] groomNames) throws IOException,
      InstantiationException, IllegalAccessException, InterruptedException {

    // set the partitioning vertex class
    conf.setClass("hama.partitioning.vertex.class", ShortestPathVertex.class,
        PartitionableWritable.class);

    return partitioner.partition(conf, fileToPartition, groomNames);

  }

}
