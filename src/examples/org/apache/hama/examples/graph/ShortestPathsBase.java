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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeerProtocol;

public abstract class ShortestPathsBase extends BSP {
  
  public static final String BSP_PEERS = "bsp.peers";
  public static final String SHORTEST_PATHS_START_VERTEX_ID = "shortest.paths.start.vertex.id";
  public static final String PARTED = "parted";
  public static final String IN_PATH = "in.path.";
  public static final String OUT_PATH = "out.path";
  public static final String NAME_VALUE_SEPARATOR = ":";
  public static final String MASTER_TASK = "master.groom";
  
  /**
   * When finished we just writing a sequencefile of the vertex name and the
   * cost.
   * 
   * @param peer The peer we got through the BSP method.
   * @param adjacencyList
   * @throws IOException
   */
  static void saveVertexMap(Configuration conf, BSPPeerProtocol peer,
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
  static void printOutput(FileSystem fs, Configuration conf) throws IOException {
    System.out.println("-------------------- RESULTS --------------------");
    FileStatus[] stati = fs.listStatus(new Path(conf.get(OUT_PATH)));
    for (FileStatus status : stati) {
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

  /**
   * 
   * The adjacencylist contains two text fields on each line. The key component
   * is the name of a vertex, the value is a ":" separated Text field that
   * contains the name of the adjacent vertex leftmost and the weight on the
   * rightmost side.
   * 
   * <PRE>
   *    K               V <br/> 
   * Vertex[Text]    AdjacentVertex : Weight [Text]
   * </PRE>
   * 
   * @param adjacencyList
   * @param vertexLookupMap
   */
  static void mapAdjacencyList(Configuration conf, BSPPeerProtocol peer,
      Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList,
      Map<String, ShortestPathVertex> vertexLookupMap)
      throws FileNotFoundException, IOException {
    FileSystem fs = FileSystem.get(conf);
    Path p = new Path(conf.get(IN_PATH
        + peer.getPeerName().split(NAME_VALUE_SEPARATOR)[0]));
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
    Text key = new Text(); // name of the vertex
    Text value = new Text(); // name of the adjacent vertex : weight
    while (reader.next(key, value)) {
      // a key vertex has weight 0 to itself
      ShortestPathVertex keyVertex = new ShortestPathVertex(0, key.toString(),
          Integer.MAX_VALUE);
      String[] nameWeight = value.toString().split(NAME_VALUE_SEPARATOR);
      if (!adjacencyList.containsKey(keyVertex)) {
        LinkedList<ShortestPathVertex> list = new LinkedList<ShortestPathVertex>();
        list.add(new ShortestPathVertex(Integer.valueOf(nameWeight[1]),
            nameWeight[0], Integer.MAX_VALUE));
        adjacencyList.put(keyVertex, list);
        vertexLookupMap.put(keyVertex.getName(), keyVertex);
      } else {
        adjacencyList.get(keyVertex).add(
            new ShortestPathVertex(Integer.valueOf(nameWeight[1]),
                nameWeight[0], Integer.MAX_VALUE));
      }
    }
    reader.close();
  }

  /**
   * Partitioning for in memory adjacency lists.
   * 
   * @param adjacencyList
   * @param status
   * @param conf
   * @return
   * @throws IOException
   */
  static HamaConfiguration partition(
      Map<ShortestPathVertex, List<ShortestPathVertex>> adjacencyList,
      HamaConfiguration conf) throws IOException {

    String[] groomNames = conf.get(BSP_PEERS).split(";");

    int sizeOfCluster = groomNames.length;

    // setup the paths where the grooms can find their input
    List<Path> partPaths = new ArrayList<Path>(sizeOfCluster);
    List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>(
        sizeOfCluster);
    FileSystem fs = FileSystem.get(conf);
    Path fileToPartition = new Path(conf.get(OUT_PATH));
    for (String entry : groomNames) {
      partPaths.add(new Path(fileToPartition.getParent().toString()
          + Path.SEPARATOR + PARTED + Path.SEPARATOR
          + entry.split(NAME_VALUE_SEPARATOR)[0]));
    }
    // create a seq writer for that
    for (Path p : partPaths) {
      // System.out.println(p.toString());
      fs.delete(p, true);
      writers.add(SequenceFile
          .createWriter(fs, conf, p, Text.class, Text.class));
    }

    for (Entry<ShortestPathVertex, List<ShortestPathVertex>> entry : adjacencyList
        .entrySet()) {
      // a key vertex has weight 0 to itself
      ShortestPathVertex keyVertex = entry.getKey();
      // just mod the id
      int mod = Math.abs(keyVertex.getId() % sizeOfCluster);
      // append it to the right sequenceFile
      for (ShortestPathVertex value : entry.getValue())
        writers.get(mod)
            .append(
                new Text(keyVertex.getName()),
                new Text(value.getName() + NAME_VALUE_SEPARATOR
                    + value.getWeight()));
    }

    for (SequenceFile.Writer w : writers)
      w.close();

    for (Path p : partPaths) {
      conf.set(IN_PATH + p.getName(), p.toString());
    }
    return conf;
  }

  /**
   * Partitioning for sequencefile partitioned adjacency lists.
   * 
   * The adjacencylist contains two text fields on each line. The key component
   * is the name of a vertex, the value is a ":" separated Text field that
   * contains the name of the adjacent vertex leftmost and the weight on the
   * rightmost side.
   * 
   * <PRE>
   *    K               V <br/> 
   * Vertex[Text]    AdjacentVertex : Weight [Text]
   * </PRE>
   * 
   * @param fileToPartition
   * @param status
   * @param conf
   * @return
   * @throws IOException
   */
  static HamaConfiguration partition(Path fileToPartition,
      HamaConfiguration conf, boolean skipPartitioning) throws IOException {

    String[] groomNames = conf.get(BSP_PEERS).split(";");
    int sizeOfCluster = groomNames.length;

    // setup the paths where the grooms can find their input
    List<Path> partPaths = new ArrayList<Path>(sizeOfCluster);
    List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>(
        sizeOfCluster);
    FileSystem fs = FileSystem.get(conf);
    for (String entry : groomNames) {
      partPaths.add(new Path(fileToPartition.getParent().toString()
          + Path.SEPARATOR + PARTED + Path.SEPARATOR
          + entry.split(NAME_VALUE_SEPARATOR)[0]));
    }

    if (!skipPartitioning) {

      // create a seq writer for that
      for (Path p : partPaths) {
        // System.out.println(p.toString());
        fs.delete(p, true);
        writers.add(SequenceFile.createWriter(fs, conf, p, Text.class,
            Text.class));
      }

      // parse our file
      if (!fs.exists(fileToPartition))
        throw new FileNotFoundException("File " + fileToPartition
            + " wasn't found!");

      SequenceFile.Reader reader = new SequenceFile.Reader(fs, fileToPartition,
          conf);
      Text key = new Text(); // name of the vertex
      Text value = new Text(); // name of the adjacent vertex : weight

      while (reader.next(key, value)) {
        // a key vertex has weight 0 to itself
        ShortestPathVertex keyVertex = new ShortestPathVertex(0, key.toString());
        // just mod the id
        int mod = Math.abs(keyVertex.getId() % sizeOfCluster);
        // append it to the right sequenceFile
        writers.get(mod).append(new Text(keyVertex.getName()), new Text(value));
      }

      reader.close();

      for (SequenceFile.Writer w : writers)
        w.close();
    }

    for (Path p : partPaths) {
      conf.set(IN_PATH + p.getName(), p.toString());
    }

    return conf;
  }

}
