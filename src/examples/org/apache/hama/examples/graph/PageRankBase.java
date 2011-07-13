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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.apache.hama.bsp.BSPPeer;

public abstract class PageRankBase extends BSP {
  public static final Log LOG = LogFactory.getLog(PageRankBase.class);

  protected static int MAX_ITERATIONS = 30;
  protected static final String MASTER_TASK = "master.groom";
  protected static double ALPHA;
  protected static int numOfVertices;
  protected static double DAMPING_FACTOR = 0.85;
  protected static double EPSILON = 0.001;

  static HashMap<PageRankVertex, List<PageRankVertex>> mapAdjacencyList(
      Configuration conf, BSPPeer peer) throws FileNotFoundException,
      IOException {
    FileSystem fs = FileSystem.get(conf);
    HashMap<PageRankVertex, List<PageRankVertex>> adjacencyList = new HashMap<PageRankVertex, List<PageRankVertex>>();
    Path p = new Path(conf.get("in.path." + peer.getPeerName().split(":")[0]));
    LOG.info(p.toString());
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
    Text key = new Text();
    Text value = new Text();
    while (reader.next(key, value)) {
      PageRankVertex k = new PageRankVertex(key.toString());
      PageRankVertex v = new PageRankVertex(value.toString());
      if (!adjacencyList.containsKey(k)) {
        adjacencyList.put(k, new LinkedList<PageRankVertex>());
        adjacencyList.get(k).add(v);
      } else {
        adjacencyList.get(k).add(v);
      }
    }
    reader.close();
    return adjacencyList;
  }

  static HamaConfiguration partitionExample(Path out, HamaConfiguration conf)
      throws IOException {

    String[] groomNames = conf.get(ShortestPaths.BSP_PEERS).split(";");
    int sizeOfCluster = groomNames.length;

    // setup the paths where the grooms can find their input
    List<Path> partPaths = new ArrayList<Path>(sizeOfCluster);
    List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>(
        sizeOfCluster);
    FileSystem fs = FileSystem.get(conf);
    for (String entry : groomNames) {
      partPaths.add(new Path(out.getParent().toString() + Path.SEPARATOR
          + ShortestPaths.PARTED + Path.SEPARATOR
          + entry.split(ShortestPaths.NAME_VALUE_SEPARATOR)[0]));
    }
    // create a seq writer for that
    for (Path p : partPaths) {
      // LOG.info(p.toString());
      fs.delete(p, true);
      writers.add(SequenceFile
          .createWriter(fs, conf, p, Text.class, Text.class));
    }

    /**
     * 1:twitter.com <br/>
     * 2:google.com <br/>
     * 3:facebook.com <br/>
     * 4:yahoo.com <br/>
     * 5:nasa.gov <br/>
     * 6:stackoverflow.com <br/>
     * 7:youtube.com
     */

    String[] realNames = new String[] { null, "twitter.com", "google.com",
        "facebook.com", "yahoo.com", "nasa.gov", "stackoverflow.com",
        "youtube.com" };

    String[] lineArray = new String[] { "1;2;3", "2", "3;1;2;5", "4;5;6",
        "5;4;6", "6;4", "7;2;4" };

    int numLines = 0;
    for (String line : lineArray) {
      String[] arr = line.split(";");
      String vId = arr[0];
      int indexKey = Integer.valueOf(vId);
      LinkedList<String> list = new LinkedList<String>();
      for (int i = 0; i < arr.length; i++) {
        int index = Integer.valueOf(arr[i]);
        list.add(realNames[index]);
      }

      int mod = Math.abs(realNames[indexKey].hashCode() % sizeOfCluster);
      for (String value : list) {
        writers.get(mod).append(new Text(realNames[indexKey]), new Text(value));
      }
      numLines++;
    }

    for (SequenceFile.Writer w : writers)
      w.close();

    for (Path p : partPaths) {
      conf.set("in.path." + p.getName(), p.toString());
    }
    conf.set("num.vertices", "" + numLines);

    return conf;
  }

  static HamaConfiguration partitionTextFile(Path in, HamaConfiguration conf)
      throws IOException {

    String[] groomNames = conf.get(ShortestPaths.BSP_PEERS).split(";");
    int sizeOfCluster = groomNames.length;

    // setup the paths where the grooms can find their input
    List<Path> partPaths = new ArrayList<Path>(sizeOfCluster);
    List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>(
        sizeOfCluster);
    FileSystem fs = FileSystem.get(conf);
    for (String entry : groomNames) {
      partPaths.add(new Path(in.getParent().toString() + Path.SEPARATOR
          + ShortestPaths.PARTED + Path.SEPARATOR
          + entry.split(ShortestPaths.NAME_VALUE_SEPARATOR)[0]));
    }
    // create a seq writer for that
    for (Path p : partPaths) {
      // LOG.info(p.toString());
      fs.delete(p, true);
      writers.add(SequenceFile
          .createWriter(fs, conf, p, Text.class, Text.class));
    }

    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(in)));

    String line = null;
    long numLines = 0;
    while ((line = br.readLine()) != null) {
      String[] arr = line.split("\t");
      String vId = arr[0];
      LinkedList<String> list = new LinkedList<String>();
      for (int i = 0; i < arr.length; i++) {
        list.add(arr[i]);
      }

      int mod = Math.abs(vId.hashCode() % sizeOfCluster);
      for (String value : list) {
        writers.get(mod).append(new Text(vId), new Text(value));
      }
      numLines++;
    }

    for (SequenceFile.Writer w : writers)
      w.close();

    for (Path p : partPaths) {
      conf.set("in.path." + p.getName(), p.toString());
    }
    conf.set("num.vertices", "" + numLines);

    return conf;
  }

  static void savePageRankMap(BSPPeer peer, Configuration conf,
      Map<PageRankVertex, Double> tentativePagerank) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path outPath = new Path(conf.get("out.path") + Path.SEPARATOR + "temp"
        + Path.SEPARATOR
        + peer.getPeerName().split(ShortestPaths.NAME_VALUE_SEPARATOR)[0]);
    fs.delete(outPath, true);
    final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf,
        outPath, Text.class, DoubleWritable.class);
    for (Entry<PageRankVertex, Double> row : tentativePagerank.entrySet()) {
      out.append(new Text(row.getKey().getUrl()), new DoubleWritable(row
          .getValue()));
    }
    LOG.info("Closing...");
    out.close();
    LOG.info("Closed!");
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
