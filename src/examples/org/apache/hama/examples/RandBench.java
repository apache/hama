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
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPPeerProtocol;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.util.Bytes;
import org.apache.zookeeper.KeeperException;

public class RandBench {
  private static final String SIZEOFMSG = "msg.size";
  private static final String N_COMMUNICATIONS = "communications.num";
  private static final String N_SUPERSTEPS = "supersteps.num";

  public static class RandBSP extends BSP {
    public static final Log LOG = LogFactory.getLog(RandBSP.class);
    private Configuration conf;
    private Random r = new Random();
    private int sizeOfMsg;
    private int nCommunications;
    private int nSupersteps;

    @Override
    public void bsp(BSPPeerProtocol bspPeer) throws IOException,
        KeeperException, InterruptedException {
      byte[] dummyData = new byte[sizeOfMsg];
      BSPMessage msg = null;
      String[] peers = bspPeer.getAllPeerNames();
      String peerName = bspPeer.getPeerName();

      for (int i = 0; i < nSupersteps; i++) {

        for (int j = 0; j < nCommunications; j++) {
          String tPeer = peers[r.nextInt(peers.length)];
          String tag = peerName + " to " + tPeer;
          msg = new BSPMessage(Bytes.toBytes(tag), dummyData);
          bspPeer.send(tPeer, msg);
        }

        bspPeer.sync();

        BSPMessage received;
        while ((received = bspPeer.getCurrentMessage()) != null) {
          LOG.info(Bytes.toString(received.getTag()) + " : " + received.getData().length);
        }
        
      }
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.sizeOfMsg = conf.getInt(SIZEOFMSG, 1);
      this.nCommunications = conf.getInt(N_COMMUNICATIONS, 1);
      this.nSupersteps = conf.getInt(N_SUPERSTEPS, 1);
    }

    @Override
    public Configuration getConf() {
      return conf;
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

    // Set the task size as a number of GroomServer
    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    bsp.setNumBspTask(cluster.getGroomServers());

    long startTime = System.currentTimeMillis();
    bsp.waitForCompletion(true);
    System.out.println("Job Finished in "
        + (double) (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");
  }
}
