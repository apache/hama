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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hama.Constants;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This class represents a BSP peer.
 */
public class BSPPeer implements Watcher, BSPPeerInterface {

  public static final Log LOG = LogFactory.getLog(BSPPeer.class);

  private Configuration conf;
  private BSPJob jobConf;

  private Server server = null;
  private ZooKeeper zk = null;
  private volatile Integer mutex = 0;

  private final String bspRoot;
  private final String quorumServers;

  private final Map<InetSocketAddress, BSPPeerInterface> peers = new ConcurrentHashMap<InetSocketAddress, BSPPeerInterface>();
  private final Map<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>();
  private ConcurrentLinkedQueue<BSPMessage> localQueue = new ConcurrentLinkedQueue<BSPMessage>();
  private ConcurrentLinkedQueue<BSPMessage> localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();
  private final Map<String, InetSocketAddress> peerSocketCache = new ConcurrentHashMap<String, InetSocketAddress>();

  private SortedSet<String> allPeerNames = new TreeSet<String>();
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
    quorumServers = QuorumPeer.getZKQuorumServersString(conf);
    LOG.debug("Quorum  " + quorumServers);

    // TODO: may require to dynamic reflect the underlying
    // network e.g. ip address, port.
    peerAddress = new InetSocketAddress(bindAddress, bindPort);

    reinitialize();
  }

  public void reinitialize() {
    try {
      LOG.debug("reinitialize(): " + getPeerName());
      server = RPC.getServer(this, peerAddress.getHostName(), peerAddress
          .getPort(), conf);
      server.start();
      LOG.info(" BSPPeer address:" + peerAddress.getHostName() + " port:"
          + peerAddress.getPort());
    } catch (IOException e) {
      LOG.error("Exception during reinitialization!", e);
    }

    try {
      zk = new ZooKeeper(quorumServers, conf.getInt(
          Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    } catch (IOException e) {
      LOG.error("Exception during reinitialization!", e);
    }

    Stat s = null;
    if (zk != null) {
      try {
        s = zk.exists(Constants.DEFAULT_ZOOKEEPER_ROOT, false);
      } catch (Exception e) {
        LOG.error(s, e);
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
    if (peerName.equals(getPeerName())) {
      LOG.debug("Local send bytes (" + msg.getData().toString() + ")");
      localQueueForNextIteration.add(msg);
    } else {
      LOG.debug("Send bytes (" + msg.getData().toString() + ") to " + peerName);
      InetSocketAddress targetPeerAddress = null;
      // Get socket for target peer.
      if (peerSocketCache.containsKey(peerName)) {
        targetPeerAddress = peerSocketCache.get(peerName);
      } else {
        targetPeerAddress = getAddress(peerName);
        peerSocketCache.put(peerName, targetPeerAddress);
      }
      ConcurrentLinkedQueue<BSPMessage> queue = outgoingQueues
          .get(targetPeerAddress);
      if (queue == null) {
        queue = new ConcurrentLinkedQueue<BSPMessage>();
      }
      queue.add(msg);
      outgoingQueues.put(targetPeerAddress, queue);
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#sync()
   */
  @Override
  public void sync() throws IOException, KeeperException, InterruptedException {
    enterBarrier();
    Iterator<Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues
        .entrySet().iterator();

    while (it.hasNext()) {
      Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> entry = it
          .next();

      BSPPeerInterface peer = peers.get(entry.getKey());
      if (peer == null) {
        peer = getBSPPeerConnection(entry.getKey());
      }
      Iterable<BSPMessage> messages = entry.getValue();
      BSPMessageBundle bundle = new BSPMessageBundle();
      for (BSPMessage message : messages) {
        bundle.addMessage(message);
      }
      peer.put(bundle);
    }

    waitForSync();
    Thread.sleep(100);

    // Clear outgoing queues.
    clearOutgoingQueues();

    currentTaskStatus.incrementSuperstepCount();
    leaveBarrier();

    // Add non-processed messages from this iteration for the next's queue.
    while (!localQueue.isEmpty()) {
      BSPMessage message = localQueue.poll();
      localQueueForNextIteration.add(message);
    }
    // Switch local queues.
    localQueue = localQueueForNextIteration;
    localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();
  }

  protected boolean enterBarrier() throws KeeperException, InterruptedException {
    LOG.debug("[" + getPeerName() + "] enter the enterbarrier");
    try {
      zk.create(bspRoot + "/" + getPeerName(), new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      LOG.error("Exception while entering barrier!", e);
    } catch (InterruptedException e) {
      LOG.error("Exception while entering barrier!", e);
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

  protected boolean waitForSync() throws KeeperException, InterruptedException {
    try {
      zk.create(bspRoot + "/" + getPeerName() + "-data", new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      LOG.error("Exception while waiting for barrier sync!", e);
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for barrier sync!", e);
    }

    while (true) {
      synchronized (mutex) {
        List<String> list = zk.getChildren(bspRoot, true);
        if (list.size() < (jobConf.getNumBspTask() * 2)) {
          mutex.wait();
        } else {
          return true;
        }
      }
    }
  }

  protected boolean leaveBarrier() throws KeeperException, InterruptedException {
    zk.delete(bspRoot + "/" + getPeerName(), 0);
    zk.delete(bspRoot + "/" + getPeerName() + "-data", 0);

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

  public void clear() {
    this.localQueue.clear();
    this.outgoingQueues.clear();
  }

  @Override
  public void close() throws IOException {
    server.stop();
  }

  @Override
  public void put(BSPMessage msg) throws IOException {
    this.localQueueForNextIteration.add(msg);
  }

  @Override
  public void put(BSPMessageBundle messages) throws IOException {
    for (BSPMessage message : messages.getMessages()) {
      this.localQueueForNextIteration.add(message);
    }
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
  public String[] getAllPeerNames() {
    return allPeerNames.toArray(new String[0]);
  }

  /**
   * To be invoked by the Groom Server with a list of peers received from an
   * heartbeat response (BSPMaster).
   * 
   * @param allPeers
   */
  void setAllPeerNames(Collection<String> allPeerNames) {
    this.allPeerNames = new TreeSet<String>(allPeerNames);
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
