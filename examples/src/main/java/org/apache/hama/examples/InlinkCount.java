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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;

import com.google.common.base.Optional;

public class InlinkCount extends Vertex<Text, NullWritable, IntWritable> {

  @Override
  public void compute(Iterable<IntWritable> messages) throws IOException {

    if (getSuperstepCount() == 0L) {
      setValue(new IntWritable(0));
      sendMessageToNeighbors(new IntWritable(1));
    } else {
      for (IntWritable msg : messages) {
        this.setValue(new IntWritable(this.getValue().get() + msg.get()));
      }
      voteToHalt();
    }
  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output> [tasks]");
    System.exit(-1);
  }

  public static GraphJob getJob(String inpath, String outpath,
      Optional<Integer> numBspTasks) throws IOException {
    // Graph job configuration
    HamaConfiguration conf = new HamaConfiguration();
    GraphJob inlinkJob = new GraphJob(conf, InlinkCount.class);
    // Set the job name
    inlinkJob.setJobName("Inlink Count");

    inlinkJob.setInputPath(new Path(inpath));
    inlinkJob.setOutputPath(new Path(outpath));

    if (numBspTasks.isPresent()) {
      inlinkJob.setNumBspTask(numBspTasks.get());
    }

    inlinkJob.setVertexClass(InlinkCount.class);

    inlinkJob.setInputFormat(TextInputFormat.class);
    inlinkJob.setInputKeyClass(Text.class);
    inlinkJob.setInputValueClass(TextArrayWritable.class);

    inlinkJob.setVertexIDClass(Text.class);
    inlinkJob.setVertexValueClass(IntWritable.class);
    inlinkJob.setEdgeValueClass(NullWritable.class);

    inlinkJob.setPartitioner(HashPartitioner.class);
    inlinkJob.setOutputFormat(SequenceFileOutputFormat.class);
    inlinkJob.setOutputKeyClass(Text.class);
    inlinkJob.setOutputValueClass(IntWritable.class);
    return inlinkJob;
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    if (args.length < 2)
      printUsage();

    Optional<Integer> absent = Optional.absent();
    GraphJob inlinkJob = getJob(args[0], args[1],
        args.length >= 3 ? Optional.of(Integer.parseInt(args[3])) : absent);

    long startTime = System.currentTimeMillis();
    if (inlinkJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
