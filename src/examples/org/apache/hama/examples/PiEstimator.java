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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPPeerProtocol;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.util.Bytes;
import org.apache.zookeeper.KeeperException;

public class PiEstimator {
  private static String MASTER_TASK = "master.task.";
  private static Path TMP_OUTPUT = new Path("/tmp/pi-example/output");

  public static class MyEstimator extends BSP {
    public static final Log LOG = LogFactory.getLog(MyEstimator.class);
    private Configuration conf;
    private String masterTask;
    private static final int iterations = 10000;

    public void bsp(BSPPeerProtocol bspPeer) throws IOException,
        KeeperException, InterruptedException {
      int in = 0, out = 0;
      for (int i = 0; i < iterations; i++) {
        double x = 2.0 * Math.random() - 1.0, y = 2.0 * Math.random() - 1.0;
        if ((Math.sqrt(x * x + y * y) < 1.0)) {
          in++;
        } else {
          out++;
        }
      }

      byte[] tagName = Bytes.toBytes(bspPeer.getPeerName());
      byte[] myData = Bytes.toBytes(4.0 * (double) in / (double) iterations);
      BSPMessage estimate = new BSPMessage(tagName, myData);

      bspPeer.send(masterTask, estimate);
      LOG.info("Send message:" + System.currentTimeMillis());
      bspPeer.sync();

      double pi = 0.0;
      BSPMessage received;
      while ((received = bspPeer.getCurrentMessage()) != null) {
        LOG.info("Receive messages:" + Bytes.toDouble(received.getData())
            + " from " + Bytes.toString(received.getTag()));
        if (pi == 0.0) {
          pi = Bytes.toDouble(received.getData());
        } else {
          pi = (pi + Bytes.toDouble(received.getData())) / 2;
        }
      }

      if (pi != 0.0) {
        FileSystem fileSys = FileSystem.get(conf);

        SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
            TMP_OUTPUT, DoubleWritable.class, DoubleWritable.class,
            CompressionType.NONE);
        writer.append(new DoubleWritable(pi), new DoubleWritable(0));
        writer.close();
      }
    }

    public Configuration getConf() {
      return conf;
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
      this.masterTask = conf.get(MASTER_TASK);
    }

  }

  public static void main(String[] args) throws InterruptedException,
      IOException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, PiEstimator.class);
    // Set the job name
    bsp.setJobName("pi estimation example");
    bsp.setBspClass(MyEstimator.class);

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    if (args.length > 0) {
      bsp.setNumBspTask(Integer.parseInt(args[0]));
    } else {
      // Set to maximum
      bsp.setNumBspTask(cluster.getGroomServers());
    }

    // Choose one as a master
    for (String peerName : cluster.getActiveGroomNames().values()) {
      conf.set(MASTER_TASK, peerName);
      break;
    }

    FileSystem fileSys = FileSystem.get(conf);
    if (fileSys.exists(TMP_OUTPUT)) {
      fileSys.delete(TMP_OUTPUT, true);
    }

    long startTime = System.currentTimeMillis();
    BSPJobClient.runJob(bsp);
    System.out.println("Job Finished in "
        + (double) (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, TMP_OUTPUT,
        conf);
    DoubleWritable output = new DoubleWritable();
    DoubleWritable zero = new DoubleWritable();
    reader.next(output, zero);
    reader.close();

    System.out.println("Estimated value of PI is " + output);
  }
}
