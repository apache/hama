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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;

public class PageRank {

  public static class PageRankVertex extends Vertex<DoubleWritable> {

    @Override
    public void compute(Iterator<DoubleWritable> messages) throws IOException {
      if (this.getSuperstepCount() == 0) {
        this.setValue(new DoubleWritable(1.0 / (double) this.getNumVertices()));
      }

      if (this.getSuperstepCount() >= 1) {
        double sum = 0;
        while (messages.hasNext()) {
          DoubleWritable msg = messages.next();
          sum += msg.get();
        }

        double ALPHA = (1 - 0.85) / (double) this.getNumVertices();
        this.setValue(new DoubleWritable(ALPHA + (0.85 * sum)));
      }

      if (this.getSuperstepCount() < this.getMaxIteration()) {
        int numEdges = this.getOutEdges().size();
        for (Edge e : this.getOutEdges()) {
          this.sendMessage(e, new DoubleWritable(this.getValue().get()
              / numEdges));
        }
      }
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

    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    GraphJob pageJob = new GraphJob(conf);
    pageJob.setJobName("Pagerank");

    pageJob.setVertexClass(PageRankVertex.class);
    pageJob.setMaxIteration(30);

    pageJob.setInputPath(new Path(args[0]));
    pageJob.setOutputPath(new Path(args[1]));

    if (args.length == 3) {
      pageJob.setNumBspTask(Integer.parseInt(args[2]));
    }

    pageJob.setInputFormat(SequenceFileInputFormat.class);
    pageJob.setPartitioner(HashPartitioner.class);
    pageJob.setOutputFormat(SequenceFileOutputFormat.class);
    pageJob.setOutputKeyClass(Text.class);
    pageJob.setOutputValueClass(DoubleWritable.class);

    long startTime = System.currentTimeMillis();
    if (pageJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
