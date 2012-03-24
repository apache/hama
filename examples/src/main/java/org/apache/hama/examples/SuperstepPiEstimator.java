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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.bsp.SuperstepBSP;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.messages.DoubleMessage;

/**
 * This PiEstimator uses the new chainable superstep API to be fault tolerant.
 */
public class SuperstepPiEstimator {

  // shared variable across multiple supersteps
  private static String masterTask;
  private static final Path TMP_OUTPUT = new Path("/tmp/pi-"
      + System.currentTimeMillis());

  public static class PiEstimatorCalculator
      extends
      Superstep<NullWritable, NullWritable, Text, DoubleWritable, DoubleMessage> {

    private static final int iterations = 10000;

    @Override
    protected void setup(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleMessage> peer) {
      super.setup(peer);
      // Choose first as a master
      masterTask = peer.getPeerName(0);
    }

    @Override
    protected void compute(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleMessage> peer)
        throws IOException {
      int in = 0;
      for (int i = 0; i < iterations; i++) {
        double x = 2.0 * Math.random() - 1.0, y = 2.0 * Math.random() - 1.0;
        if ((Math.sqrt(x * x + y * y) < 1.0)) {
          in++;
        }
      }

      double data = 4.0 * (double) in / (double) iterations;
      DoubleMessage estimate = new DoubleMessage(peer.getPeerName(), data);

      peer.send(masterTask, estimate);
    }
  }

  protected static class PiEstimatorAggregator
      extends
      Superstep<NullWritable, NullWritable, Text, DoubleWritable, DoubleMessage> {

    @Override
    protected void compute(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleMessage> peer)
        throws IOException {
      if (peer.getPeerName().equals(masterTask)) {
        double pi = 0.0;
        int numPeers = peer.getNumCurrentMessages();
        DoubleMessage received;
        while ((received = (DoubleMessage) peer.getCurrentMessage()) != null) {
          pi += received.getData();
        }

        pi = pi / numPeers;
        peer.write(new Text("Estimated value of PI is"), new DoubleWritable(pi));
      }
    }

    @Override
    protected boolean haltComputation(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleMessage> peer) {
      return true;
    }

  }

  private static void printOutput(HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(TMP_OUTPUT);
    for (FileStatus file : files) {
      if (file.getLen() > 0) {
        FSDataInputStream in = fs.open(file.getPath());
        IOUtils.copyBytes(in, System.out, conf, false);
        in.close();
        break;
      }
    }

    fs.delete(TMP_OUTPUT, true);
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException,
      ClassNotFoundException, InterruptedException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    BSPJob bsp = new BSPJob(conf, SuperstepBSP.class);
    // Set the job name
    bsp.setJobName("Fault Tolerant Pi Estimation Example");

    // set our supersteps, they must be given in execution order
    bsp.setSupersteps(SuperstepPiEstimator.PiEstimatorCalculator.class,
        SuperstepPiEstimator.PiEstimatorAggregator.class);

    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (args.length > 0) {
      bsp.setNumBspTask(Integer.parseInt(args[0]));
    } else {
      // Set to maximum
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      printOutput(conf);
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }

}
