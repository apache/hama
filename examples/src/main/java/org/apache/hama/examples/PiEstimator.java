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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.zookeeper.KeeperException;

public class PiEstimator {
  private static String MASTER_TASK = "master.task.";
  private static Path TMP_OUTPUT = new Path("/tmp/pi-example/output");

  public static class MyEstimator extends BSP {
    public static final Log LOG = LogFactory.getLog(MyEstimator.class);
    private String masterTask;
    private static final int iterations = 10000;

    @Override
    public void setup(BSPPeer peer) {
      this.masterTask = conf.get(MASTER_TASK);
    }

    @Override
    public void bsp(BSPPeer bspPeer) throws IOException, KeeperException,
        InterruptedException {

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
      DoubleMessage estimate = new DoubleMessage(bspPeer.getPeerName(), data);

      bspPeer.send(masterTask, estimate);
      bspPeer.sync();

      if (bspPeer.getPeerName().equals(masterTask)) {
        double pi = 0.0;
        int numPeers = bspPeer.getNumCurrentMessages();
        DoubleMessage received;
        while ((received = (DoubleMessage) bspPeer.getCurrentMessage()) != null) {
          pi += received.getData();
        }

        pi = pi / numPeers;
        writeResult(pi);
      }
    }

    private void writeResult(double pi) throws IOException {
      FileSystem fileSys = FileSystem.get(conf);

      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
          TMP_OUTPUT, DoubleWritable.class, DoubleWritable.class,
          CompressionType.NONE);
      writer.append(new DoubleWritable(pi), new DoubleWritable(0));
      writer.close();
    }
  }

  private static void initTempDir(FileSystem fileSys) throws IOException {
    if (fileSys.exists(TMP_OUTPUT)) {
      fileSys.delete(TMP_OUTPUT, true);
    }
  }

  private static void printOutput(FileSystem fileSys, HamaConfiguration conf)
      throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, TMP_OUTPUT,
        conf);
    DoubleWritable output = new DoubleWritable();
    DoubleWritable zero = new DoubleWritable();
    reader.next(output, zero);
    reader.close();

    System.out.println("Estimated value of PI is " + output);
  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, PiEstimator.class);
    // Set the job name
    bsp.setJobName("Pi Estimation Example");
    bsp.setBspClass(MyEstimator.class);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (args.length > 0) {
      bsp.setNumBspTask(Integer.parseInt(args[0]));
    } else {
      // Set to maximum
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    // Choose one as a master
    for (String hostName : cluster.getActiveGroomNames().keySet()) {
      conf.set(MASTER_TASK, hostName);
      break;
    }

    FileSystem fileSys = FileSystem.get(conf);
    initTempDir(fileSys);

    long startTime = System.currentTimeMillis();

    if (bsp.waitForCompletion(true)) {
      printOutput(fileSys, conf);

      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
