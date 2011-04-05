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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junit.framework.AssertionFailedError;
import net.sourceforge.groboutils.junit.v1.MultiThreadedTestRunner;
import net.sourceforge.groboutils.junit.v1.TestRunnable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class TestBSPPeer extends HamaCluster implements Watcher {
  private Log LOG = LogFactory.getLog(TestBSPPeer.class);

  private static final int NUM_PEER = 10;
  private static final int ROUND = 3;
  private static final int PAYLOAD = 1024; // 1kb in default
  List<BSPPeerThread> list = new ArrayList<BSPPeerThread>(NUM_PEER);
  HamaConfiguration conf;
  private Random r = new Random();

  public TestBSPPeer() {
    this.conf = getConf();
  }

  public void setUp() throws Exception {
    super.setUp();
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

  public class BSPPeerThread extends TestRunnable {
    private BSPPeer peer;
    private int MAXIMUM_DURATION = 5;
    private int lastTwoDigitsOfPort;

    public BSPPeerThread(HamaConfiguration conf) throws IOException {
      lastTwoDigitsOfPort = conf.getInt(Constants.PEER_PORT, 0) - 30000;
      this.peer = new BSPPeer(conf);
      Set<String> peerNames = new HashSet<String>(NUM_PEER);
      for (int i = 0; i < NUM_PEER; i++) {
        peerNames.add("localhost:" + (30000 + i));
      }
      peer.setAllPeerNames(peerNames);
      TaskStatus currentTaskStatus = new TaskStatus(new BSPJobID(),
          new TaskAttemptID(), 0, null, null, null, null);
      peer.setCurrentTaskStatus(currentTaskStatus);
      BSPJob jobConf = new BSPJob(conf, NUM_PEER);
      peer.setJobConf((BSPJob) jobConf);
    }

    @Override
    public void runTest() throws AssertionFailedError {
      int randomTime;
      byte[] dummyData = new byte[PAYLOAD];
      BSPMessage msg = null;

      for (int i = 0; i < ROUND; i++) {
        randomTime = r.nextInt(MAXIMUM_DURATION) + 5;

        for (int j = 0; j < 10; j++) {
          r.nextBytes(dummyData);
          msg = new ByteMessage(Bytes.tail(dummyData, 128), dummyData);
          String peerName = "localhost:" + (30000 + j);
          try {
            peer.send(peerName, msg);
          } catch (IOException e) {
            LOG.info(e);
          }
        }

        try {
          Thread.sleep(randomTime * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        try {
          peer.sync();
        } catch (IOException e) {
          e.printStackTrace();
        } catch (KeeperException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        verifyPayload(i);
      }
    }

    private void verifyPayload(int round) {
      int numMessages = peer.getNumCurrentMessages();
      assertEquals(round, ((int) peer.getSuperstepCount() - 1));

      LOG.info("[" + peer.getPeerName() + "] verifying " + numMessages
          + " messages at " + round + " round");

      if (lastTwoDigitsOfPort < 10) {
        assertEquals(10, numMessages);
      } else {
        assertEquals(0, numMessages);
      }

      ByteMessage msg = null;
      int messageCounter = 0;

      try {
        while ((msg = (ByteMessage) peer.getCurrentMessage()) != null) {
          assertEquals(
              Bytes.compareTo(msg.getTag(), 0, 128, msg.getData(),
                  msg.getData().length - 128, 128), 0);
          ++messageCounter;
        }
      } catch (IOException e) {
        LOG.error(e);
      }
      assertEquals(numMessages, messageCounter);

      peer.clearLocalQueue();
    }

    public BSPPeer getBSPPeer() {
      return this.peer;
    }
  }

  public void testSync() throws Throwable {

    conf.setInt("bsp.peers.num", NUM_PEER);
    conf.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    conf.set(Constants.PEER_HOST, "localhost");
    conf.set(Constants.ZOOKEEPER_SERVER_ADDRS, "localhost:21810");

    TestRunnable[] threads = new TestRunnable[NUM_PEER];

    for (int i = 0; i < NUM_PEER; i++) {
      conf.set(Constants.PEER_PORT, String.valueOf(30000 + i));
      threads[i] = new BSPPeerThread(conf);
    }

    MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(threads);
    mttr.runTestRunnables();
  }

  @Override
  public void process(WatchedEvent event) {
  }
}
