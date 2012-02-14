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
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

public class SSSP {
  public static final String START_VERTEX = "shortest.paths.start.vertex.name";

  public static class ShortestPathVertex extends Vertex<IntWritable> {

    public ShortestPathVertex() {
      this.setValue(new IntWritable(Integer.MAX_VALUE));
    }

    public boolean isStartVertex() {
      String startVertex = getConf().get(START_VERTEX);
      return (this.getVertexID().equals(startVertex)) ? true : false;
    }

    @Override
    public void compute(Iterator<IntWritable> messages) throws IOException {
      int minDist = isStartVertex() ? 0 : Integer.MAX_VALUE;

      while (messages.hasNext()) {
        IntWritable msg = messages.next();
        if (msg.get() < minDist) {
          minDist = msg.get();
        }
      }

      if (minDist < this.getValue().get()) {
        this.setValue(new IntWritable(minDist));
        for (Edge e : this.getOutEdges()) {
          sendMessage(e, new IntWritable(minDist + e.getCost()));
        }
      }
    }
  }

  private static void printUsage() {
    System.out.println("Usage: <startnode> <input> <output> [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (args.length < 3)
      printUsage();

    // Graph job configuration
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob ssspJob = new GraphJob(conf);
    // Set the job name
    ssspJob.setJobName("Single Source Shortest Path");

    conf.set(START_VERTEX, args[0]);
    ssspJob.setInputPath(new Path(args[1]));
    ssspJob.setOutputPath(new Path(args[2]));

    if (args.length == 4) {
      ssspJob.setNumBspTask(Integer.parseInt(args[3]));
    }

    ssspJob.setVertexClass(ShortestPathVertex.class);
    ssspJob.setInputFormat(SequenceFileInputFormat.class);
    ssspJob.setInputKeyClass(VertexWritable.class);
    ssspJob.setInputValueClass(VertexArrayWritable.class);

    ssspJob.setPartitioner(HashPartitioner.class);
    ssspJob.setOutputFormat(SequenceFileOutputFormat.class);
    ssspJob.setOutputKeyClass(Text.class);
    ssspJob.setOutputValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    if (ssspJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
