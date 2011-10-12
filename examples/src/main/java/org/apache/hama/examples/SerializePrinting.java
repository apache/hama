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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.zookeeper.KeeperException;

public class SerializePrinting {
  private static String TMP_OUTPUT = "/tmp/test-example/";

  public static class HelloBSP extends BSP {
    public static final Log LOG = LogFactory.getLog(HelloBSP.class);
    private final static int PRINT_INTERVAL = 1000;
    private FileSystem fileSys;
    private int num;

    @Override
    public void setup(BSPPeer peer) {
      num = Integer.parseInt(conf.get("bsp.peers.num"));
      try {
        fileSys = FileSystem.get(conf);
      } catch (IOException e) {
        throw new Error("Filesystem could not be initialized! ", e);
      }
    }

    @Override
    public void bsp(BSPPeer bspPeer) throws IOException, KeeperException,
        InterruptedException {

      LOG.info(bspPeer.getAllPeerNames());
      int i = 0;
      for (String otherPeer : bspPeer.getAllPeerNames()) {
        String peerName = bspPeer.getPeerName();
        if (peerName.equals(otherPeer)) {
          writeLogToFile(peerName, i);
        }

        Thread.sleep(PRINT_INTERVAL);
        bspPeer.sync();
        i++;
      }
    }

    private void writeLogToFile(String string, int i) throws IOException {
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
          new Path(TMP_OUTPUT + i), LongWritable.class, Text.class,
          CompressionType.NONE);
      writer.append(new LongWritable(System.currentTimeMillis()), new Text(
          "Hello BSP from " + (i + 1) + " of " + num + ": " + string));
      writer.close();
    }
  }

  private static void printOutput(FileSystem fileSys, ClusterStatus cluster,
      HamaConfiguration conf) throws IOException {
    System.out.println("Each task printed the \"Hello World\" as below:");
    for (int i = 0; i < cluster.getGroomServers(); i++) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(
          TMP_OUTPUT + i), conf);
      LongWritable timestamp = new LongWritable();
      Text message = new Text();
      reader.next(timestamp, message);
      System.out.println(new Date(timestamp.get()) + ": " + message);
      reader.close();
    }
  }

  private static void initTempDir(FileSystem fileSys) throws IOException {
    if (fileSys.exists(new Path(TMP_OUTPUT))) {
      fileSys.delete(new Path(TMP_OUTPUT), true);
    }
  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, SerializePrinting.class);
    // Set the job name
    bsp.setJobName("Serialize Printing");
    bsp.setBspClass(HelloBSP.class);

    // Set the task size as a number of GroomServer
    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    bsp.setNumBspTask(cluster.getGroomServers());

    FileSystem fileSys = FileSystem.get(conf);
    initTempDir(fileSys);

    if (bsp.waitForCompletion(true)) {
      printOutput(fileSys, cluster, conf);
    }
  }

}
