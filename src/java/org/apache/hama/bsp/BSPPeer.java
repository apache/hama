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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import org.apache.zookeeper.data.Stat;

public class BSPPeer implements Watcher, BSPPeerInterface {
  public static final Log LOG = LogFactory.getLog(BSPPeer.class);

  private Configuration conf;
  private BSPJob jobConf;

  private Server server = null;
  private ZooKeeper zk = null;
  private volatile Integer mutex = 0;

  private final String bspRoot;
  private final String zookeeperAddr;

  private final Map<InetSocketAddress, BSPPeerInterface> peers = 
    new ConcurrentHashMap<InetSocketAddress, BSPPeerInterface>();
  private final Map<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = 
    new ConcurrentHashMap<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>();
  private final ConcurrentLinkedQueue<BSPMessage> localQueue = 
    new ConcurrentLinkedQueue<BSPMessage>();
  private Set<String> allPeerNames = new HashSet<String>();
  private InetSocketAddress peerAddress;
  private TaskStatus currentTaskStatus;

  /**
   * Constructor
   */
  public BSPPeer(Configuration conf) throws IOException {
    this.conf = conf;
    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);
    zookeeperAddr = conf.get(Constants.ZOOKEEPER_QUORUM)
        + ":"
        + conf.getInt(Constants.ZOOKEPER_CLIENT_PORT,
            Constants.DEFAULT_ZOOKEPER_CLIENT_PORT);
    // TODO: may require to dynamic reflect the underlying 
    //       network e.g. ip address, port.
    peerAddress = new InetSocketAddress(bindAddress, bindPort);

    reinitialize();
  }

  public void reinitialize() {
    try {
      LOG.debug("reinitialize(): " + getPeerName());
      server = RPC.getServer(this, peerAddress.getHostName(), peerAddress
          .getPort(), conf);
      server.start();
      LOG.info(" BSPPeer address:"+peerAddress.getHostName()+" port:"+peerAddress.getPort());
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      zk = new ZooKeeper(zookeeperAddr, 3000, this);
    } catch (IOException e) {
      e.printStackTrace();
    }

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
  public void send(String peerName, BSPMessage msg) throws IOException {
    LOG.debug("Send bytes (" + msg.getData().toString() + ") to " + peerName);
    ConcurrentLinkedQueue<BSPMessage> queue = outgoingQueues.get(peerName);
    if (queue == null) {
      queue = new ConcurrentLinkedQueue<BSPMessage>();
      outgoingQueues.put(getAddress(peerName), queue);
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

    // Clear outgoing queues.
    clearOutgoingQueues();

    enterBarrier();
    Thread.sleep(Constants.ATLEAST_WAIT_TIME); // TODO - This is temporary work
    // because
    // it can be affected by network condition,
    // the number of peers, and the load of zookeeper.
    // It should fixed to some flawless way.
    leaveBarrier();
    currentTaskStatus.incrementSuperstepCount();
  }

  protected boolean enterBarrier() throws KeeperException, InterruptedException {
    LOG.debug("[" + getPeerName() + "] enter the enterbarrier");
    try {
      zk.create(bspRoot + "/" + getPeerName(), new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    while (true) {
      synchronized (mutex) {
        List<String> list = zk.getChildren(bspRoot, true);

        if (list.size() < jobConf.getNumBspTask()) {
          mutex.wait();
        } else {
          return true;
        }
      }
    }
  }

  protected boolean leaveBarrier() throws KeeperException, InterruptedException {
    zk.delete(bspRoot + "/" + getPeerName(), 0);

    while (true) {
      synchronized (mutex) {
        List<String> list = zk.getChildren(bspRoot, true);
        if (list.size() > 0) {
          mutex.wait();
        } else {
          LOG.debug("[" + getPeerName() + "] leave from the leaveBarrier");
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
    server.stop();
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

  /**
   * @return the string as host:port of this Peer
   */
  public String getPeerName() {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  private InetSocketAddress getAddress(String peerName) {
    String[] peerAddrParts = peerName.split(":");
    return new InetSocketAddress(peerAddrParts[0], Integer
        .parseInt(peerAddrParts[1]));
  }

  @Override
  public Set<String> getAllPeerNames() {
    return allPeerNames;
  }

  /**
   * To be invoked by the Groom Server with a list of peers received from an
   * heartbeat response (BSPMaster).
   * 
   * @param allPeers
   */
  void setAllPeerNames(Collection<String> allPeerNames) {
    this.allPeerNames = new HashSet<String>(allPeerNames);
  }

  /**
   * @return the number of messages
   */
  public int getNumCurrentMessages() {
    return localQueue.size();
  }

  /**
   * Sets the current status
   * 
   * @param currentTaskStatus
   */
  public void setCurrentTaskStatus(TaskStatus currentTaskStatus) {
    this.currentTaskStatus = currentTaskStatus;
  }

  /**
   * @return the count of current super-step
   */
  public long getSuperstepCount() {
    return currentTaskStatus.getSuperstepCount();
  }

  /**
   * Sets the job configuration
   * 
   * @param jobConf
   */
  public void setJobConf(BSPJob jobConf) {
    this.jobConf = jobConf;
  }

  /**
   * @return the size of local queue
   */
  public int getLocalQueueSize() {
    return localQueue.size();
  }

  /**
   * @return the size of outgoing queue
   */
  public int getOutgoingQueueSize() {
    return outgoingQueues.size();
  }

  /**
   * Clears local queue
   */
  public void clearLocalQueue() {
    this.localQueue.clear();
  }
  
  /**
   * Clears outgoing queues
   */
  public void clearOutgoingQueues() {
    this.outgoingQueues.clear();
  }
}
