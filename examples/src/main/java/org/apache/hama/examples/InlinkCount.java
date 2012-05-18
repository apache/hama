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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

public class InlinkCount extends Vertex<Text, IntWritable, NullWritable> {

  @Override
  public void compute(Iterator<IntWritable> messages) throws IOException {

    if (getSuperstepCount() == 0L) {
      sendMessageToNeighbors(new IntWritable(1));
    } else {
      while (messages.hasNext()) {
        IntWritable msg = messages.next();
        this.setValue(new IntWritable(this.getValue().get() + msg.get()));
      }
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
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
    inlinkJob.setInputFormat(SequenceFileInputFormat.class);
    inlinkJob.setInputKeyClass(VertexWritable.class);
    inlinkJob.setInputValueClass(VertexArrayWritable.class);
    
    inlinkJob.setVertexIDClass(Text.class);
    inlinkJob.setVertexValueClass(IntWritable.class);
    inlinkJob.setEdgeValueClass(NullWritable.class);

    inlinkJob.setPartitioner(HashPartitioner.class);
    inlinkJob.setOutputFormat(SequenceFileOutputFormat.class);
    inlinkJob.setOutputKeyClass(Text.class);
    inlinkJob.setOutputValueClass(IntWritable.class);

    long startTime = System.currentTimeMillis();
    if (inlinkJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
