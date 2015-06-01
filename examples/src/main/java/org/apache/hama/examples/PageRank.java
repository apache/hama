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
import java.util.Iterator;

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

/**
 * Real pagerank with dangling node contribution.
 */
public class PageRank {

  public static class PageRankVertex extends
      Vertex<Text, NullWritable, DoubleWritable> {

    static double DAMPING_FACTOR = 0.85;
    static double MAXIMUM_CONVERGENCE_ERROR = 0.001;

    @Override
    public void setup(HamaConfiguration conf) {
      String val = conf.get("hama.pagerank.alpha");
      if (val != null) {
        DAMPING_FACTOR = Double.parseDouble(val);
      }
      val = conf.get("hama.graph.max.convergence.error");
      if (val != null) {
        MAXIMUM_CONVERGENCE_ERROR = Double.parseDouble(val);
      }
      
      // initialize this vertex to 1 / count of global vertices in this graph
      setValue(new DoubleWritable(1.0 / getTotalNumVertices()));
    }

    @Override
    public void compute(Iterable<DoubleWritable> messages) throws IOException {
      if (this.getSuperstepCount() >= 1) {
        double sum = 0;
        for (DoubleWritable msg : messages) {
          sum += msg.get();
        }
        double alpha = (1.0d - DAMPING_FACTOR) / getTotalNumVertices();
        setValue(new DoubleWritable(alpha + (sum * DAMPING_FACTOR)));
        aggregate(0, this.getValue());
      }

      // if we have not reached our global error yet, then proceed.
      DoubleWritable globalError = getAggregatedValue(0);

      if (globalError != null && this.getSuperstepCount() > 2
          && MAXIMUM_CONVERGENCE_ERROR > globalError.get()) {
        voteToHalt();
      } else {
        // in each superstep we are going to send a new rank to our neighbours
        sendMessageToNeighbors(new DoubleWritable(this.getValue().get()
            / this.getEdges().size()));
      }
    }
  }

  public static class PagerankTextReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, DoubleWritable> {

    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, DoubleWritable> vertex) throws Exception {

      String[] tokenArray = value.toString().split("\t");
      String vtx = tokenArray[0].trim();
      String[] edges = tokenArray[1].trim().split(" ");

      vertex.setVertexID(new Text(vtx));

      for (String v : edges) {
        vertex.addEdge(new Edge<Text, NullWritable>(new Text(v), null));
      }

      return true;
    }
  }

  public static class PagerankJsonReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, DoubleWritable> {

    @SuppressWarnings("unchecked")
    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, DoubleWritable> vertex) throws Exception {
      JSONArray jsonArray = (JSONArray) new JSONParser()
          .parse(value.toString());

      vertex.setVertexID(new Text(jsonArray.get(0).toString()));

      Iterator<JSONArray> iter = ((JSONArray) jsonArray.get(2)).iterator();
      while (iter.hasNext()) {
        JSONArray edge = (JSONArray) iter.next();
        vertex.addEdge(new Edge<Text, NullWritable>(new Text(edge.get(0)
            .toString()), null));
      }

      return true;
    }
  }

  public static GraphJob createJob(String[] args, HamaConfiguration conf,
      Options opts) throws IOException, ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (!cliParser.hasOption("i") || !cliParser.hasOption("o")) {
      System.out
          .println("No input or output path specified for PageRank, exiting.");
    }

    GraphJob pageJob = new GraphJob(conf, PageRank.class);
    pageJob.setJobName("Pagerank");

    pageJob.setVertexClass(PageRankVertex.class);
    pageJob.setInputPath(new Path(cliParser.getOptionValue("i")));
    pageJob.setOutputPath(new Path(cliParser.getOptionValue("o")));

    // set the defaults
    pageJob.setMaxIteration(30);
    pageJob.set("hama.pagerank.alpha", "0.85");
    // reference vertices to itself, because we don't have a dangling node
    // contribution here
    pageJob.set("hama.graph.self.ref", "true");
    pageJob.set("hama.graph.max.convergence.error", "0.001");

    if (cliParser.hasOption("t")) {
      pageJob.setNumBspTask(Integer.parseInt(cliParser.getOptionValue("t")));
    }

    // error
    pageJob.setAggregatorClass(AverageAggregator.class);

    // Vertex reader
    // According to file type, which is Text or Json,
    // Vertex reader handle it differently.
    if (cliParser.hasOption("f")) {
      if (cliParser.getOptionValue("f").equals("text")) {
        pageJob.setVertexInputReaderClass(PagerankTextReader.class);
      } else if (cliParser.getOptionValue("f").equals("json")) {
        pageJob.setVertexInputReaderClass(PagerankJsonReader.class);
      } else {
        System.out.println("File type is not available to run Pagerank... "
            + "File type set default value, Text.");
        pageJob.setVertexInputReaderClass(PagerankTextReader.class);
      }
    } else {
      pageJob.setVertexInputReaderClass(PagerankTextReader.class);
    }

    pageJob.setVertexIDClass(Text.class);
    pageJob.setVertexValueClass(DoubleWritable.class);
    pageJob.setEdgeValueClass(NullWritable.class);

    pageJob.setInputFormat(TextInputFormat.class);
    pageJob.setInputKeyClass(LongWritable.class);
    pageJob.setInputValueClass(Text.class);

    pageJob.setPartitioner(HashPartitioner.class);
    pageJob.setOutputFormat(TextOutputFormat.class);
    pageJob.setOutputKeyClass(Text.class);
    pageJob.setOutputValueClass(DoubleWritable.class);
    return pageJob;
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, ParseException {
    Options opts = new Options();
    opts.addOption("i", "input_path", true, "The Location of output path.");
    opts.addOption("o", "output_path", true, "The Location of input path.");
    opts.addOption("h", "help", false, "Print usage");
    opts.addOption("t", "task_num", true, "The number of tasks.");
    opts.addOption("f", "file_type", true, "The file type of input data. Input"
        + "file format which is \"text\" tab delimiter separated or \"json\"."
        + "Default value - Text");

    if (args.length < 2) {
      new HelpFormatter().printHelp("pagerank -i INPUT_PATH -o OUTPUT_PATH "
          + "[-t NUM_TASKS] [-f FILE_TYPE]", opts);
      System.exit(-1);
    }

    HamaConfiguration conf = new HamaConfiguration();
    GraphJob pageJob = createJob(args, conf, opts);

    long startTime = System.currentTimeMillis();
    if (pageJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
