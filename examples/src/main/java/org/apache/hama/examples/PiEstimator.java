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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.sync.SyncException;

public class PiEstimator {
  private static Path TMP_OUTPUT = new Path("/tmp/pi-temp");

  public static class MyEstimator extends
      BSP<NullWritable, NullWritable, Text, DoubleWritable> {
    public static final Log LOG = LogFactory.getLog(MyEstimator.class);
    private String masterTask;
    private static final int iterations = 10000;

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable> peer)
        throws IOException, SyncException, InterruptedException {

      int in = 0, out = 0;
      for (int i = 0; i < iterations; i++) {
        double x = 2.0 * Math.random() - 1.0, y = 2.0 * Math.random() - 1.0;
        if ((Math.sqrt(x * x + y * y) < 1.0)) {
          in++;
        } else {
          out++;
        }
      }

      double data = 4.0 * (double) in / (double) iterations;
      DoubleMessage estimate = new DoubleMessage(peer.getPeerName(), data);

      peer.send(masterTask, estimate);
      peer.sync();
    }

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable> peer)
        throws IOException {
      // Choose one as a master
      this.masterTask = peer.getPeerName(peer.getNumPeers() / 2);
    }

    public void cleanup(
        BSPPeer<NullWritable, NullWritable, Text, DoubleWritable> peer)
        throws IOException {
      if (peer.getPeerName().equals(masterTask)) {
        double pi = 0.0;
        int numPeers = peer.getNumCurrentMessages();
        DoubleMessage received;
        while ((received = (DoubleMessage) peer.getCurrentMessage()) != null) {
          pi += received.getData();
        }

        pi = pi / numPeers;
        peer
            .write(new Text("Estimated value of PI is"), new DoubleWritable(pi));
      }
    }
  }

  static void printOutput(HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(TMP_OUTPUT);
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() > 0) {
        FSDataInputStream in = fs.open(files[i].getPath());
        IOUtils.copyBytes(in, System.out, conf, false);
        in.close();
        break;
      }
    }

    fs.delete(TMP_OUTPUT, true);
  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, PiEstimator.class);
    // Set the job name
    bsp.setJobName("Pi Estimation Example");
    bsp.setBspClass(MyEstimator.class);
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
