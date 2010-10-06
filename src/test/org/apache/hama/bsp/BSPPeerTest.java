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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class BSPPeerTest extends HamaCluster implements Watcher {
  private Log LOG = LogFactory.getLog(BSPPeerTest.class);

  private static final int NUM_PEER = 35;
  private static final int ROUND = 3;
  private static final int PAYLOAD = 1024; // 1kb in default
  List<BSPPeerThread> list = new ArrayList<BSPPeerThread>(NUM_PEER);
  Configuration conf;
  private Random r = new Random();

  public BSPPeerTest() {
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

  public class BSPPeerThread extends Thread {
    private BSPPeer peer;
    private int MAXIMUM_DURATION = 5;    

    public BSPPeerThread(Configuration conf) throws IOException {
      this.peer = new BSPPeer(conf);
    }

    @Override
    public void run() {
      int randomTime;
      byte[] dummyData = new byte[PAYLOAD];
      BSPMessage msg = null;
      InetSocketAddress addr = null;

      for (int i = 0; i < ROUND; i++) {
        randomTime = r.nextInt(MAXIMUM_DURATION) + 5;

        for (int j = 0; j < 10; j++) {
          r.nextBytes(dummyData);
          msg = new BSPMessage(Bytes.tail(dummyData, 128), dummyData);

          addr = new InetSocketAddress("localhost", 30000 + j);
          try {
            peer.send(addr, msg);
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

        verifyPayload();
      }
    }

    private void verifyPayload() {
      System.out.println("[" + getName() + "] verifying "
          + peer.localQueue.size() + " messages");
      BSPMessage msg = null;

      try {
        while ((msg = peer.getCurrentMessage()) != null) {
          assertEquals(Bytes.compareTo(msg.tag, 0, 128, msg.data,
              msg.data.length - 128, 128), 0);
        }
      } catch (IOException e) {
        LOG.error(e);
      }

      peer.localQueue.clear();
    }
    
    public BSPPeer getBSPPeer() {
      return this.peer;
    }
  }

  public void testSync() throws InterruptedException, IOException {

    BSPPeerThread thread;
    for (int i = 0; i < NUM_PEER; i++) {
      conf.setInt("bsp.peers.num", NUM_PEER);
      conf.set(Constants.PEER_HOST, "localhost");
      conf.set(Constants.PEER_PORT, String.valueOf(30000 + i));
      conf.set(Constants.ZOOKEEPER_SERVER_ADDRS, "localhost:21810");
      thread = new BSPPeerThread(conf);
      list.add(thread);
    }

    for (int i = 0; i < NUM_PEER; i++) {
      list.get(i).start();
    }

    for (int i = 0; i < NUM_PEER; i++) {
      list.get(i).join();
    }
  }
  
  /*
   * Test method for constructors
   */
  public void testBSPPeer() throws IOException {
    Configuration conf = new Configuration();    
    BSPPeer peer = new BSPPeer(conf);
    
    System.out.println(peer.bindAddress+" = "+Constants.DEFAULT_PEER_HOST);
    System.out.println(peer.bindPort+" = "+Constants.DEFAULT_PEER_PORT);
    assertEquals(peer.bindAddress,Constants.DEFAULT_PEER_HOST);
    assertEquals(peer.bindPort,Constants.DEFAULT_PEER_PORT);
    assertEquals(peer.zookeeperAddr,Constants.DEFAULT_ZOOKEEPER_SERVER_ADDR);
    
    int peerPort;
    int zkPort;
    conf = new Configuration();
    conf.set(Constants.PEER_HOST, "localhost");
    do{      
      peerPort = r.nextInt(Short.MAX_VALUE);
    } while(peerPort == 0);    
    conf.setInt(Constants.PEER_PORT, peerPort);
    
    do{      
      zkPort = r.nextInt(Short.MAX_VALUE);
    } while(zkPort == peerPort || zkPort == 0);    
    conf.set(Constants.ZOOKEEPER_SERVER_ADDRS, "localhost:"+zkPort);
    peer = new BSPPeer(conf);
    assertEquals(peer.bindAddress,"localhost");
    assertEquals(peer.bindPort,peerPort);
    assertEquals(peer.zookeeperAddr,"localhost:"+zkPort);
  }

  @Override
  public void process(WatchedEvent event) {
  }
}
