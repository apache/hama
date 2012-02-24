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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.NullOutputFormat;
import org.apache.hama.bsp.sync.SyncException;

public class RandBench {
  private static final String SIZEOFMSG = "msg.size";
  private static final String N_COMMUNICATIONS = "communications.num";
  private static final String N_SUPERSTEPS = "supersteps.num";

  public static class RandBSP
      extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, BytesWritable> {
    public static final Log LOG = LogFactory.getLog(RandBSP.class);
    private Random r = new Random();
    private int sizeOfMsg;
    private int nCommunications;
    private int nSupersteps;

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, BytesWritable> peer)
        throws IOException, SyncException, InterruptedException {
      byte[] dummyData = new byte[sizeOfMsg];
      String[] peers = peer.getAllPeerNames();

      for (int i = 0; i < nSupersteps; i++) {

        for (int j = 0; j < nCommunications; j++) {
          String tPeer = peers[r.nextInt(peers.length)];
          BytesWritable data = new BytesWritable();
          data.set(dummyData, 0, dummyData.length);
          peer.send(tPeer, data);
        }

        peer.sync();

        BytesWritable received;
        while ((received = peer.getCurrentMessage()) != null) {
          LOG.info(received.getBytes().length);
        }

      }
    }

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, BytesWritable> peer) {
      this.sizeOfMsg = peer.getConfiguration().getInt(SIZEOFMSG, 1);
      this.nCommunications = peer.getConfiguration().getInt(N_COMMUNICATIONS, 1);
      this.nSupersteps = peer.getConfiguration().getInt(N_SUPERSTEPS, 1);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Usage: <sizeOfMsg> <nCommunications> <nSupersteps>");
      System.exit(-1);
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    conf.setInt(SIZEOFMSG, Integer.parseInt(args[0]));
    conf.setInt(N_COMMUNICATIONS, Integer.parseInt(args[1]));
    conf.setInt(N_SUPERSTEPS, Integer.parseInt(args[2]));

    BSPJob bsp = new BSPJob(conf, RandBench.class);
    // Set the job name
    bsp.setJobName("Random Communication Benchmark");
    bsp.setBspClass(RandBSP.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputFormat(NullOutputFormat.class);

    // Set the task size as a number of GroomServer
    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    bsp.setNumBspTask(cluster.getMaxTasks());

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (double) (System.currentTimeMillis() - startTime) / 1000.0
          + " seconds");
    }
  }
}
