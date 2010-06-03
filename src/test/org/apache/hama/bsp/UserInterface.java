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
package org.apache.hama.bsp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class UserInterface extends HamaCluster implements Watcher {
  private HamaConfiguration conf;
  private int NUM_PEER = 10;
  List<PiEstimator> list = new ArrayList<PiEstimator>(NUM_PEER);
  
  class PiEstimator extends BSP {
    private static final int iterations = 10000;

    public PiEstimator(Configuration conf) throws IOException {
      super(conf);
    }

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

      byte[] tagName = Bytes.toBytes(getName().toString());
      byte[] myData = Bytes.toBytes(4.0 * (double) in / (double) iterations);
      BSPMessage estimate = new BSPMessage(tagName, myData);

      bspPeer.send(new InetSocketAddress("localhost", 30000), estimate);
      bspPeer.sync();

      double pi = 0.0;
      BSPMessage received;
      while ((received = bspPeer.getCurrentMessage()) != null) {
        pi = (pi + Bytes.toDouble(received.getData())) / 2;
      }

      if (pi != 0.0)
        System.out.println(bspPeer.getServerName() + ": " + pi);
    }
  }

  public void setUp() throws Exception {
    super.setUp();
    this.conf = getConf();
    ZooKeeper zk = new ZooKeeper("localhost:21810", 3000, this);
    Stat s = null;
    if (zk != null) {
      try {
        s = zk.exists(Constants.DEFAULT_ZOOKEEPER_ROOT, false);
      } catch (Exception e) {
        LOG.error(s);
      }

      if (s == null) {
        try {
          zk.create(Constants.DEFAULT_ZOOKEEPER_ROOT, new byte[0],
              Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          LOG.error(e);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }
    }
  }

  public void testBSPMain() throws InterruptedException, IOException {
    PiEstimator thread;
    for (int i = 0; i < NUM_PEER; i++) {
      conf.set("bsp.peers.num", String.valueOf(NUM_PEER));
      conf.set(Constants.PEER_HOST, "localhost");
      conf.set(Constants.PEER_PORT, String.valueOf(30000 + i));
      conf.set(Constants.ZOOKEEPER_SERVER_ADDRS, "localhost:21810");
      thread = new PiEstimator(conf);
      list.add(thread);
    }

    for (int i = 0; i < NUM_PEER; i++) {
      list.get(i).start();
    }

    for (int i = 0; i < NUM_PEER; i++) {
      list.get(i).join();
    }

    /*
    // BSP job configuration
    BSPJob bsp = new BSPJob(this.conf);
    // Set the job name
    bsp.setJobName("bsp test job");
    bsp.setBspClass(PiEstimator.class);
    bsp.submit();
    */
  }

  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub

  }
}
