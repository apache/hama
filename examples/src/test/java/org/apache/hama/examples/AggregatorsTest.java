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
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.SumAggregator;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.junit.Test;

/**
 * Unit test for aggregators
 */
public class AggregatorsTest extends TestCase {
  private static String OUTPUT = "/tmp/page-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void verifyResult() throws IOException {
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(fts.getPath())));
      String line = null;

      String[] results = { "6.0", "2.0", "3.0", "4.0" };

      for (int i = 1; i < 5; i++) {
        line = reader.readLine();
        String[] split = line.split("\t");
        assertTrue(split[0].equals(String.valueOf(i)));
        assertTrue(split[1].equals(results[i - 1]));
        System.out.println(split[0] + " : " + split[1]);
      }
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  @Test
  public void test() throws IOException, InterruptedException,
      ClassNotFoundException {
    try {
      CustomAggregators
          .main(new String[] { "src/test/resources/dg.txt", OUTPUT });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  static class CustomAggregators {

    public static class GraphTextReader
        extends
        VertexInputReader<LongWritable, Text, Text, NullWritable, DoubleWritable> {

      @Override
      public boolean parseVertex(LongWritable key, Text value,
          Vertex<Text, NullWritable, DoubleWritable> vertex) throws Exception {

        vertex.setVertexID(value);
        vertex
            .setValue(new DoubleWritable(Double.parseDouble(value.toString())));

        return true;
      }
    }

    public static class GraphVertex extends
        Vertex<Text, NullWritable, DoubleWritable> {

      @Override
      public void compute(Iterable<DoubleWritable> msgs) throws IOException {

        // We will send 2 custom messages on superstep 2 and 4 only!
        if (this.getSuperstepCount() == 2) {
          this.aggregate("mySum", new DoubleWritable(1.0));
        }

        if (this.getSuperstepCount() == 4) {
          this.aggregate("mySum", new DoubleWritable(2.0));
        }

        // We will get the first aggrigation result from our custom aggregator
        // on superstep 3,
        // and we will store the result only in vertex 4.
        // This vertex should have value = 4.
        if (this.getSuperstepCount() == 3
            && this.getVertexID().toString().equals("4")) {
          this.setValue((DoubleWritable) this.getAggregatedValue("mySum"));
        }

        // By setting vertex number 3 to halt, we will see a change on the
        // aggregating results
        // in both custom and global aggregators.
        // This vertex should have value = 3.
        if (this.getSuperstepCount() == 3
            && this.getVertexID().toString().equals("3")) {
          this.voteToHalt();
        }

        // This vertex should have value = 6 (3 vertices are working x 2 the
        // custom value)
        if (this.getSuperstepCount() == 5
            && this.getVertexID().toString().equals("1")) {
          this.setValue((DoubleWritable) this.getAggregatedValue("mySum"));
        }

        if (this.getSuperstepCount() == 6) {
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
      GraphJob graphJob = new GraphJob(conf, CustomAggregators.class);
      graphJob.setJobName("Custom Aggregators");
      graphJob.setVertexClass(GraphVertex.class);

      graphJob.registerAggregator("mySum", SumAggregator.class);

      graphJob.setInputPath(new Path(args[0]));
      graphJob.setOutputPath(new Path(args[1]));

      graphJob.setVertexIDClass(Text.class);
      graphJob.setVertexValueClass(DoubleWritable.class);
      graphJob.setEdgeValueClass(NullWritable.class);

      graphJob.setInputFormat(TextInputFormat.class);

      graphJob.setVertexInputReaderClass(GraphTextReader.class);
      graphJob.setPartitioner(HashPartitioner.class);

      graphJob.setOutputFormat(TextOutputFormat.class);
      graphJob.setOutputKeyClass(Text.class);
      graphJob.setOutputValueClass(DoubleWritable.class);

      return graphJob;
    }
  }
}
