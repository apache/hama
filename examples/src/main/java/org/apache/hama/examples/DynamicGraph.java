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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.GraphJobRunner.GraphJobCounter;
import org.apache.hama.graph.MapVerticesInfo;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * NOTE: Graph modification APIs can be used only with {@link MapVerticesInfo}.
 * 
 * This is an example of how to manipulate Graphs dynamically. The input of this
 * example is a number in each row. We assume that the is a vertex with ID:1
 * which is responsible to create a sum vertex that will aggregate the values of
 * the other vertices. During the aggregation, sum vertex will delete all other
 * vertices.
 * 
 * Input example: 1 2 3 4
 * 
 * Output example: sum 10
 */
public class DynamicGraph {

  public static class GraphTextReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, IntWritable> {

    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, IntWritable> vertex) throws Exception {

      vertex.setVertexID(value);
      vertex.setValue(new IntWritable(Integer.parseInt(value.toString())));

      return true;
    }
  }

  public static class GraphVertex extends
      Vertex<Text, NullWritable, IntWritable> {

    private void createSumVertex() throws IOException {
      if (this.getVertexID().toString().equals("1")) {
        Text new_id = new Text("sum");
        this.addVertex(new_id, new ArrayList<Edge<Text, NullWritable>>(),
            new IntWritable(0));
      }
    }

    private void sendAllValuesToSumAndRemove() throws IOException {
      if (!this.getVertexID().toString().equals("sum")) {
        this.sendMessage(new Text("sum"), this.getValue());
        this.remove();
      }
    }

    // this must run only on "sum" vertex
    private void calculateSum(Iterable<IntWritable> msgs) throws IOException {
      if (this.getVertexID().toString().equals("sum")) {
        int s = 0;
        for (IntWritable i : msgs) {
          s += i.get();
        }
        s += this.getPeer().getCounter(GraphJobCounter.INPUT_VERTICES)
            .getCounter();
        this.setValue(new IntWritable(this.getValue().get() + s));
      } else {
        throw new UnsupportedOperationException(
            "We have more vertecies than we expected: " + this.getVertexID()
                + " " + this.getValue());
      }
    }

    @Override
    public void compute(Iterable<IntWritable> msgs) throws IOException {
      if (this.getSuperstepCount() == 0) {
        createSumVertex();
      } else if (this.getSuperstepCount() == 1) {
        sendAllValuesToSumAndRemove();
      } else if (this.getSuperstepCount() == 2) {
        calculateSum(msgs);
      } else if (this.getSuperstepCount() == 3) {
        this.voteToHalt();
      }
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length != 2) {
      printUsage();
    }
    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    GraphJob graphJob = createJob(args, conf);
    long startTime = System.currentTimeMillis();
    if (graphJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output>");
    System.exit(-1);
  }

  private static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {

    // NOTE: Graph modification APIs can be used only with MapVerticesInfo.
    conf.set("hama.graph.vertices.info",
        "org.apache.hama.graph.MapVerticesInfo");

    GraphJob graphJob = new GraphJob(conf, DynamicGraph.class);
    graphJob.setJobName("Dynamic Graph");
    graphJob.setVertexClass(GraphVertex.class);

    graphJob.setInputPath(new Path(args[0]));
    graphJob.setOutputPath(new Path(args[1]));

    graphJob.setVertexIDClass(Text.class);
    graphJob.setVertexValueClass(IntWritable.class);
    graphJob.setEdgeValueClass(NullWritable.class);

    graphJob.setInputFormat(TextInputFormat.class);

    graphJob.setVertexInputReaderClass(GraphTextReader.class);
    graphJob.setPartitioner(HashPartitioner.class);

    graphJob.setOutputFormat(TextOutputFormat.class);
    graphJob.setOutputKeyClass(Text.class);
    graphJob.setOutputValueClass(IntWritable.class);

    return graphJob;
  }
}
