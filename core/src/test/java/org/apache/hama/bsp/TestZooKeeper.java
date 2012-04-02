/**
 * Copyright 2007 The Apache Software Foundation
 *
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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.ClassSerializePrinting;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class TestZooKeeper extends HamaCluster {

  private static Log LOG = LogFactory.getLog(TestZooKeeper.class);

  private HamaConfiguration configuration;

  public TestZooKeeper() {
    configuration = new HamaConfiguration();
    configuration.set("bsp.master.address", "localhost");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.set("bsp.local.dir", "/tmp/hama-test");
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT, 21810);
    configuration.set("hama.sync.client.class",
        org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl.class
            .getCanonicalName());
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void testClearZKNodes() throws IOException, KeeperException,
      InterruptedException {

    // Clear any existing znode with the same path as bspRoot.
    bspCluster.getBSPMaster().clearZKNodes();

    int timeout = configuration.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT,
        6000);
    String connectStr = QuorumPeer.getZKQuorumServersString(configuration);
    String bspRoot = configuration.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);

    // Establishing a zk session.
    ZooKeeper zk = new ZooKeeper(connectStr, timeout, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // Do nothing.(Dummy Watcher)
      }
    });

    // Creating dummy bspRoot if it doesn't already exist.
    Stat s = zk.exists(bspRoot, false);
    if (s == null) {
      zk.create(bspRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    }

    // Creating dummy child nodes at depth 1.
    String node1 = bspRoot + "/task1";
    String node2 = bspRoot + "/task2";
    zk.create(node1, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(node2, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Creating dummy child node at depth 2.
    String node11 = node1 + "/superstep1";
    zk.create(node11, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    ArrayList<String> list = (ArrayList<String>) zk.getChildren(bspRoot, false);
    assertEquals(2, list.size());
    System.out.println(list.size());

    bspCluster.getBSPMaster().clearZKNodes();

    list = (ArrayList<String>) zk.getChildren(bspRoot, false);
    System.out.println(list.size());
    assertEquals(0, list.size());

    try {
      zk.getData(node11, false, null);
      fail();
    } catch (KeeperException.NoNodeException e) {
      System.out.println("Node has been removed correctly!");
    }
  }

}
