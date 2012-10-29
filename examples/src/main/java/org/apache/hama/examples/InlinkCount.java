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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class InlinkCount extends Vertex<Text, NullWritable, IntWritable> {

  @Override
  public void compute(Iterator<IntWritable> messages) throws IOException {

    if (getSuperstepCount() == 0L) {
      setValue(new IntWritable(0));
      sendMessageToNeighbors(new IntWritable(1));
    } else {
      while (messages.hasNext()) {
        IntWritable msg = messages.next();
        this.setValue(new IntWritable(this.getValue().get() + msg.get()));
      }
      voteToHalt();
    }
  }

  public static class InlinkCountTextReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, IntWritable> {

    /**
     * The text file essentially should look like: <br/>
     * VERTEX_ID\t(n-tab separated VERTEX_IDs)<br/>
     * E.G:<br/>
     * 1\t2\t3\t4<br/>
     * 2\t3\t1<br/>
     * etc.
     */
    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, IntWritable> vertex) throws Exception {
      String[] split = value.toString().split("\t");
      for (int i = 0; i < split.length; i++) {
        if (i == 0) {
          vertex.setVertexID(new Text(split[i]));
        } else {
          vertex
              .addEdge(new Edge<Text, NullWritable>(new Text(split[i]), null));
        }
      }
      return true;
    }

  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output> [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    if (args.length < 2)
      printUsage();

    // Graph job configuration
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob inlinkJob = new GraphJob(conf, InlinkCount.class);
    // Set the job name
    inlinkJob.setJobName("Inlink Count");

    inlinkJob.setInputPath(new Path(args[0]));
    inlinkJob.setOutputPath(new Path(args[1]));

    if (args.length == 3) {
      inlinkJob.setNumBspTask(Integer.parseInt(args[2]));
    }

    inlinkJob.setVertexClass(InlinkCount.class);
    inlinkJob.setInputFormat(TextInputFormat.class);
    inlinkJob.setInputKeyClass(LongWritable.class);
    inlinkJob.setInputValueClass(Text.class);

    inlinkJob.setVertexIDClass(Text.class);
    inlinkJob.setVertexValueClass(IntWritable.class);
    inlinkJob.setEdgeValueClass(NullWritable.class);
    inlinkJob.setVertexInputReaderClass(InlinkCountTextReader.class);

    inlinkJob.setPartitioner(HashPartitioner.class);
    inlinkJob.setOutputFormat(SequenceFileOutputFormat.class);
    inlinkJob.setOutputKeyClass(Text.class);
    inlinkJob.setOutputValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    if (inlinkJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
