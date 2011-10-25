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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.checkpoint.CheckpointRunner;
import org.apache.hama.ipc.BSPPeerProtocol;

/**
 * This class represents a BSP peer.
 */
public class BSPPeerImpl implements BSPPeer {

  public static final Log LOG = LogFactory.getLog(BSPPeerImpl.class);

  private final Configuration conf;
  private BSPJob bspJob;

  private volatile Server server = null;

  private final Map<InetSocketAddress, BSPPeer> peers = new ConcurrentHashMap<InetSocketAddress, BSPPeer>();
  private final Map<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>();
  private ConcurrentLinkedQueue<BSPMessage> localQueue = new ConcurrentLinkedQueue<BSPMessage>();
  private ConcurrentLinkedQueue<BSPMessage> localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();
  private final Map<String, InetSocketAddress> peerSocketCache = new ConcurrentHashMap<String, InetSocketAddress>();

  private InetSocketAddress peerAddress;
  private TaskStatus currentTaskStatus;

  private TaskAttemptID taskId;
  private BSPPeerProtocol umbilical;

  private final BSPMessageSerializer messageSerializer;

  private String[] allPeers;

  private SyncClient syncClient;

  /**
   * Protected default constructor for LocalBSPRunner.
   */
  protected BSPPeerImpl() {
    messageSerializer = null;
    conf = null;
  }

  /**
   * BSPPeer Constructor.
   * 
   * BSPPeer acts on behalf of clients performing bsp() tasks.
   * 
   * @param conf is the configuration file containing bsp peer host, port, etc.
   * @param umbilical is the bsp protocol used to contact its parent process.
   * @param taskId is the id that current process holds.
   * @throws Exception
   */
  public BSPPeerImpl(BSPJob job, Configuration conf, TaskAttemptID taskId,
      BSPPeerProtocol umbilical) throws Exception {
    this.conf = conf;
    this.taskId = taskId;
    this.umbilical = umbilical;
    this.bspJob = job;

    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    peerAddress = new InetSocketAddress(bindAddress, bindPort);
    BSPMessageSerializer msgSerializer = null;
    if (this.conf.getBoolean("bsp.checkpoint.enabled", false)) {
      msgSerializer = new BSPMessageSerializer(conf,
          conf.getInt("bsp.checkpoint.port",
              Integer.valueOf(CheckpointRunner.DEFAULT_PORT)));
    }
    this.messageSerializer = msgSerializer;
    initialize();
    syncClient.register(taskId.getJobID(), taskId, peerAddress.getHostName(),
        peerAddress.getPort());
    // initial barrier syncing to get all the hosts to the same point, to get
    // consistent peernames.
    syncClient.enterBarrier(taskId.getJobID(), taskId, -1);
    syncClient.leaveBarrier(taskId.getJobID(), taskId, -1);
    setCurrentTaskStatus(new TaskStatus(taskId.getJobID(), taskId, 0,
        TaskStatus.State.RUNNING, "running", peerAddress.getHostName(),
        TaskStatus.Phase.STARTING));
  }

  public void initialize() throws Exception {
    try {
      if (LOG.isDebugEnabled())
        LOG.debug("reinitialize(): " + getPeerName());
      this.server = RPC.getServer(this, peerAddress.getHostName(),
          peerAddress.getPort(), conf);
      server.start();
      LOG.info(" BSPPeer address:" + peerAddress.getHostName() + " port:"
          + peerAddress.getPort());
    } catch (IOException e) {
      LOG.error("Fail to start RPC server!", e);
    }

    syncClient = SyncServiceFactory.getSyncClient(conf);
    syncClient.init(conf, taskId.getJobID(), taskId);

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

  private String checkpointedPath() {
    String backup = conf.get("bsp.checkpoint.prefix_path", "/checkpoint/");
    String ckptPath = backup + bspJob.getJobID().toString() + "/"
        + getSuperstepCount() + "/" + this.taskId.toString();
    if (LOG.isDebugEnabled())
      LOG.debug("Messages are to be saved to " + ckptPath);
    return ckptPath;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#sync()
   */
  @Override
  public void sync() throws InterruptedException {
    try {
      enterBarrier();
      Iterator<Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues
          .entrySet().iterator();

      while (it.hasNext()) {
        Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> entry = it
            .next();

        BSPPeer peer = getBSPPeerConnection(entry.getKey());
        Iterable<BSPMessage> messages = entry.getValue();
        BSPMessageBundle bundle = new BSPMessageBundle();

        Combiner combiner = (Combiner) ReflectionUtils.newInstance(
            conf.getClass("bsp.combiner.class", Combiner.class), conf);

        if (combiner != null) {
          bundle = combiner.combine(messages);
        } else {
          for (BSPMessage message : messages) {
            bundle.addMessage(message);
          }
        }

        // checkpointing
        if (null != this.messageSerializer) {
          this.messageSerializer.serialize(new BSPSerializableMessage(
              checkpointedPath(), bundle));
        }

        peer.put(bundle);
      }

      leaveBarrier();
      currentTaskStatus.incrementSuperstepCount();
      umbilical.statusUpdate(taskId, currentTaskStatus);

      // Clear outgoing queues.
      clearOutgoingQueues();

      // Add non-processed messages from this iteration for the next's queue.
      while (!localQueue.isEmpty()) {
        BSPMessage message = localQueue.poll();
        localQueueForNextIteration.add(message);
      }
      // Switch local queues.
      localQueue = localQueueForNextIteration;
      localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();
    } catch (Exception e) {
      LOG.fatal(
          "Caught exception during superstep "
              + currentTaskStatus.getSuperstepCount() + "!", e);
      // throw new RuntimeException(e);
    }
  }

  protected void enterBarrier() throws Exception {
    syncClient.enterBarrier(taskId.getJobID(), taskId,
        currentTaskStatus.getSuperstepCount());
  }

  protected void leaveBarrier() throws Exception {
    syncClient.leaveBarrier(taskId.getJobID(), taskId,
        currentTaskStatus.getSuperstepCount());
  }

  public void clear() {
    this.localQueue.clear();
    this.outgoingQueues.clear();
  }

  public void close() throws Exception {
    this.clear();
    syncClient.close();
    if (server != null)
      server.stop();
    if (null != messageSerializer)
      this.messageSerializer.close();
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
    return BSPPeer.versionID;
  }

  protected BSPPeer getBSPPeerConnection(InetSocketAddress addr)
      throws NullPointerException, IOException {
    BSPPeer peer;
    peer = peers.get(addr);
    if (peer == null) {
      synchronized (this.peers) {
        peer = (BSPPeer) RPC.getProxy(BSPPeer.class, BSPPeer.versionID, addr,
            this.conf);
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
    if (peerAddrParts.length != 2) {
      throw new ArrayIndexOutOfBoundsException(
          "Peername must consist of exactly ONE \":\"! Given peername was: "
              + peerName);
    }
    return new InetSocketAddress(peerAddrParts[0],
        Integer.valueOf(peerAddrParts[1]));
  }

  @Override
  public String[] getAllPeerNames() {
    initPeerNames();
    return allPeers;
  }

  @Override
  public String getPeerName(int index) {
    initPeerNames();
    return allPeers[index];
  }

  @Override
  public int getNumPeers() {
    initPeerNames();
    return allPeers.length;
  }

  private void initPeerNames() {
    if (allPeers == null) {
      allPeers = syncClient.getAllPeerNames(taskId);
    }
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
   * Gets the job configuration.
   * 
   * @return the conf
   */
  public Configuration getConfiguration() {
    return conf;
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
