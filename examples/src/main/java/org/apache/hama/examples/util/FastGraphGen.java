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
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.*;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.TextArrayWritable;

import com.google.common.collect.Sets;

import org.json.simple.JSONArray;

public class FastGraphGen {
  protected static Log LOG = LogFactory.getLog(FastGraphGen.class);

  private static String SIZE_OF_MATRIX = "size.of.matrix";
  private static String MAX_EDGES = "max.outlinks";
  private static String OUTPUT_FORMAT = "graph.outputformat";
  private static String WEIGHT = "graph.weight";

  public static class FastGraphGenBSP extends
      BSP<NullWritable, NullWritable, Text, TextArrayWritable, Text> {

    private Configuration conf;
    private int sizeN;
    private int maxOutEdges;
    private boolean isJson;
    private int weight;

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer) {
      this.conf = peer.getConfiguration();
      sizeN = conf.getInt(SIZE_OF_MATRIX, 10);
      maxOutEdges = conf.getInt(MAX_EDGES, 3);
      isJson = conf.getBoolean(OUTPUT_FORMAT, false);
      weight = conf.getInt(WEIGHT, 0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer)
        throws IOException, SyncException, InterruptedException {
      int interval = sizeN / peer.getNumPeers();
      int startID = peer.getPeerIndex() * interval;
      int endID;
      if (peer.getPeerIndex() == peer.getNumPeers() - 1) {
        endID = sizeN;
      } else {
        endID = startID + interval;
      }

      Random r = new Random();
      if (isJson) {
        for (int i = startID; i < endID; i++) {

          JSONArray vtxArray = new JSONArray();
          vtxArray.add(i);
          vtxArray.add(0);
          JSONArray edgeArray = new JSONArray();
          HashSet<Integer> set = Sets.newHashSet();
          for (int j = 0; j < maxOutEdges; j++) {
            set.add(r.nextInt(sizeN));
          }
          for (int x : set) {
            JSONArray edge = new JSONArray();
            edge.add(x);
            if (weight == 0)
              edge.add(0);
            else if (weight > 0)
              edge.add(r.nextInt(weight));
            edgeArray.add(edge);
          }
          vtxArray.add(edgeArray);
          peer.write(new Text(vtxArray.toString()), null);
        }

      } else {
        for (int i = startID; i < endID; i++) {
          HashSet<Integer> set = Sets.newHashSet();
          for (int j = 0; j < maxOutEdges; j++) {
            set.add(r.nextInt(sizeN));
          }

          TextArrayWritable textArrayWritable = new TextArrayWritable();
          Text[] arr = new Text[set.size()];
          int index = 0;
          for (int x : set) {
            arr[index++] = new Text(String.valueOf(x));
          }
          textArrayWritable.set(arr);

          peer.write(new Text(String.valueOf(i)), textArrayWritable);
        }
      }
    }
  }

  public static void main(String[] args)
      throws InterruptedException, IOException, ClassNotFoundException,
      ParseException {
    Options opts = new Options();
    opts.addOption("v", "vertices", true, "The total number of vertices. Default value is 10.");
    opts.addOption("e", "edges", true, "The maximum number of edges per vertex. Default value is 3.");
    opts.addOption("o", "output_path", true, "The Location of output path.");
    opts.addOption("t", "task_num", true, "The number of tasks. Default value is one.");
    opts.addOption("h", "help", false, "Print usage");
    opts.addOption("of", "output_format", true, "OutputFormat Type which is \"text\", "
        + "tab delimiter separated or \"json\". Default value - text");
    opts.addOption("w", "weight", true, "Enable to set weight of graph edges."
        + "Default value - 0.");

    CommandLine cliParser = new GnuParser().parse(opts, args);

    // outputType, that has a value of "Text" unless true,
    // when it has a value of "Json".
    boolean outputType = false;

    if (args.length == 0) {
      new HelpFormatter().printHelp("gen -o OUTPUT_PATH [options]", opts);
      System.exit(-1);
    }

    if (cliParser.hasOption("h")) {
      new HelpFormatter().printHelp("FastGraphGen -o OUTPUT_PATH [options]", opts);
      return;
    }

    if (!cliParser.hasOption("o")) {
      System.out.println("No output path specified for FastGraphGen, exiting.");
      System.exit(-1);
    }

    if (cliParser.hasOption("of")) {
      if (cliParser.getOptionValue("of").equals("json"))
        outputType = true;
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    conf.setInt(SIZE_OF_MATRIX, Integer.parseInt(cliParser.getOptionValue("vertices", "5")));
    conf.setInt(MAX_EDGES,
        Integer.parseInt(cliParser.getOptionValue("edges", "3")));
    conf.setBoolean(OUTPUT_FORMAT, outputType);
    conf.setInt(WEIGHT, Integer.parseInt(cliParser.getOptionValue("weight", "1")));

    BSPJob bsp = new BSPJob(conf, FastGraphGenBSP.class);
    // Set the job name
    bsp.setJobName("Random Fast Matrix Generator");
    bsp.setBspClass(FastGraphGenBSP.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(TextArrayWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, new Path(cliParser.getOptionValue("output_path")));
    bsp.setNumBspTask(Integer.parseInt(cliParser.getOptionValue("task_num", "1")));

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
