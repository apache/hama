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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hama.Constants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class BSPPeer implements DefaultBSPPeer, Watcher, BSPPeerInterface {
  public static final Log LOG = LogFactory.getLog(BSPPeer.class);

  protected Configuration conf;

  protected InetSocketAddress masterAddr = null;
  protected Server server = null;
  protected ZooKeeper zk = null;
  protected volatile Integer mutex = 0;

  protected final String serverName;
  protected final String bindAddress;
  protected final int bindPort;
  protected final String bspRoot;
  protected final String zookeeperAddr;

  protected final Map<InetSocketAddress, BSPPeerInterface> peers = new ConcurrentHashMap<InetSocketAddress, BSPPeerInterface>();
  protected final Map<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>();
  protected final ConcurrentLinkedQueue<BSPMessage> localQueue = new ConcurrentLinkedQueue<BSPMessage>();

  /**
   * 
   */
  public BSPPeer(Configuration conf) throws IOException {
    this.conf = conf;

    serverName = conf.get(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST)
        + ":" + conf.getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    bindAddress = conf.get(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST);
    bindPort = conf.getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);
    zookeeperAddr = conf.get(Constants.ZOOKEEPER_SERVER_ADDRS,
        "localhost:21810");

    reinitialize();
  }

  public void reinitialize() {
    try {
      System.out.println(bindAddress + ":" + bindPort);
      server = RPC.getServer(this, bindAddress, bindPort, conf);
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      zk = new ZooKeeper(zookeeperAddr, 3000, this);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public BSPMessage getCurrentMessage() throws IOException {
    return localQueue.poll();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#send(java.net.InetSocketAddress,
   * org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable)
   */
  @Override
  public void send(InetSocketAddress hostname, BSPMessage msg)
      throws IOException {

    ConcurrentLinkedQueue<BSPMessage> queue = outgoingQueues.get(hostname);
    if (queue == null) {
      queue = new ConcurrentLinkedQueue<BSPMessage>();
      outgoingQueues.put(hostname, queue);
    }
    queue.add(msg);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#sync()
   */
  @Override
  public void sync() throws IOException, KeeperException, InterruptedException {
    Iterator<Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues
        .entrySet().iterator();
    Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> entry;
    ConcurrentLinkedQueue<BSPMessage> queue;
    BSPPeerInterface peer;

    Iterator<BSPMessage> messages;

    while (it.hasNext()) {
      entry = it.next();

      peer = peers.get(entry.getKey());
      if (peer == null) {
        peer = getBSPPeerConnection(entry.getKey());
      }
      queue = entry.getValue();
      messages = queue.iterator();

      // TODO - to be improved by collective communication and compression
      while (messages.hasNext()) {
        peer.put(messages.next());
      }
    }
    enterBarrier();
    Thread.sleep(Constants.ATLEAST_WAIT_TIME); // TODO - This is temporary work because
    // it can be affected by network condition,
    // the number of peers, and the load of zookeeper.
    // It should fixed to some flawless way.
    leaveBarrier();

  }

  protected boolean enterBarrier() throws KeeperException, InterruptedException {
    LOG.debug("[" + serverName + "] enter the enterbarrier");
    try {
      zk.create(bspRoot + "/" + serverName, new byte[0], Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    while (true) {
      synchronized (mutex) {
        List<String> list = zk.getChildren(bspRoot, true);
        if (list.size() < Integer.valueOf(conf.get("bsp.peers.num"))) {
          mutex.wait();
        } else {
          return true;
        }
      }
    }
  }

  protected boolean leaveBarrier() throws KeeperException, InterruptedException {
    zk.delete(bspRoot + "/" + serverName, 0);

    while (true) {
      synchronized (mutex) {
        List<String> list = zk.getChildren(bspRoot, true);
        if (list.size() > 0) {
          mutex.wait();
        } else {
          LOG.debug("[" + serverName + "] leave from the leaveBarrier");
          return true;
        }
      }
    }
  }

  @Override
  public void process(WatchedEvent event) {
    synchronized (mutex) {
      mutex.notify();
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isRunning() {
    return true;
  }

  @Override
  public void put(BSPMessage msg) throws IOException {
    this.localQueue.add(msg);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return BSPPeerInterface.versionID;
  }

  protected BSPPeerInterface getBSPPeerConnection(InetSocketAddress addr) {
    BSPPeerInterface peer;
    synchronized (this.peers) {
      peer = peers.get(addr);

      if (peer == null) {
        try {
          peer = (BSPPeerInterface) RPC.getProxy(BSPPeerInterface.class,
              BSPPeerInterface.versionID, addr, this.conf);
        } catch (IOException e) {

        }
        this.peers.put(addr, peer);
      }
    }

    return peer;
  }
}
