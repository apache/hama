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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hama.Constants;
import org.apache.hama.checkpoint.CheckpointRunner;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.Bytes;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * This class represents a BSP peer.
 */
public class BSPPeer implements Watcher, BSPPeerInterface {

  public static final Log LOG = LogFactory.getLog(BSPPeer.class);

  private final Configuration conf;
  private BSPJob jobConf;

  private volatile Server server = null;
  private ZooKeeper zk = null;
  private volatile Integer mutex = 0;

  private final String bspRoot;
  private final String quorumServers;

  private final Map<InetSocketAddress, BSPPeerInterface> peers = new ConcurrentHashMap<InetSocketAddress, BSPPeerInterface>();
  private final Map<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>();
  private ConcurrentLinkedQueue<BSPMessage> localQueue = new ConcurrentLinkedQueue<BSPMessage>();
  private ConcurrentLinkedQueue<BSPMessage> localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();
  private final Map<String, InetSocketAddress> peerSocketCache = new ConcurrentHashMap<String, InetSocketAddress>();

  private InetSocketAddress peerAddress;
  private TaskStatus currentTaskStatus;

  private TaskAttemptID taskid;
  private BSPPeerProtocol umbilical;

  private final BSPMessageSerializer messageSerializer;

  public static final class BSPSerializableMessage implements Writable {
    final AtomicReference<String> path = new AtomicReference<String>();
    final AtomicReference<BSPMessageBundle> bundle = new AtomicReference<BSPMessageBundle>();

    public BSPSerializableMessage() {
    }

    public BSPSerializableMessage(final String path,
        final BSPMessageBundle bundle) {
      if (null == path)
        throw new NullPointerException("No path provided for checkpointing.");
      if (null == bundle)
        throw new NullPointerException("No data provided for checkpointing.");
      this.path.set(path);
      this.bundle.set(bundle);
    }

    public final String checkpointedPath() {
      return this.path.get();
    }

    public final BSPMessageBundle messageBundle() {
      return this.bundle.get();
    }

    @Override
    public final void write(DataOutput out) throws IOException {
      out.writeUTF(this.path.get());
      this.bundle.get().write(out);
    }

    @Override
    public final void readFields(DataInput in) throws IOException {
      this.path.set(in.readUTF());
      BSPMessageBundle pack = new BSPMessageBundle();
      pack.readFields(in);
      this.bundle.set(pack);
    }

  }// serializable message

  final class BSPMessageSerializer {
    final Socket client;
    final ScheduledExecutorService sched;

    public BSPMessageSerializer(final int port) {
      Socket tmp = null;
      int cnt = 0;
      do {
        tmp = init(port);
        cnt++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          LOG.warn("Thread is interrupted.", ie);
          Thread.currentThread().interrupt();
        }
      } while (null == tmp && 10 > cnt);
      this.client = tmp;
      if (null == this.client)
        throw new NullPointerException("Client socket is null.");
      this.sched = Executors.newScheduledThreadPool(conf.getInt(
          "bsp.checkpoint.serializer_thread", 10));
      LOG.info(BSPMessageSerializer.class.getName()
          + " is ready to serialize message.");
    }

    private Socket init(final int port) {
      Socket tmp = null;
      try {
        tmp = new Socket("localhost", port);
      } catch (UnknownHostException uhe) {
        LOG.error("Unable to connect to BSPMessageDeserializer.", uhe);
      } catch (IOException ioe) {
        LOG.warn("Fail to create socket.", ioe);
      }
      return tmp;
    }

    void serialize(final BSPSerializableMessage tmp) throws IOException {
      if (LOG.isDebugEnabled())
        LOG.debug("Messages are saved to " + tmp.checkpointedPath());
      final DataOutput out = new DataOutputStream(client.getOutputStream());
      this.sched.schedule(new Callable<Object>() {
        public Object call() throws Exception {
          tmp.write(out);
          return null;
        }
      }, 0, SECONDS);
    }

    public void close() {
      try {
        this.client.close();
        this.sched.shutdown();
      } catch (IOException io) {
        LOG.error("Fail to close client socket.", io);
      }
    }

  }// message serializer

  /**
   * Protected default constructor for LocalBSPRunner.
   */
  protected BSPPeer() {
    bspRoot = null;
    quorumServers = null;
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
   * @param taskid is the id that current process holds.
   */
  public BSPPeer(Configuration conf, TaskAttemptID taskid,
      BSPPeerProtocol umbilical) throws IOException {
    this.conf = conf;
    this.taskid = taskid;
    this.umbilical = umbilical;

    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);
    quorumServers = QuorumPeer.getZKQuorumServersString(conf);
    if (LOG.isDebugEnabled())
      LOG.debug("Quorum  " + quorumServers);
    peerAddress = new InetSocketAddress(bindAddress, bindPort);
    BSPMessageSerializer msgSerializer = null;
    if (this.conf.getBoolean("bsp.checkpoint.enabled", false)) {
      msgSerializer = new BSPMessageSerializer(conf.getInt(
          "bsp.checkpoint.port", Integer
              .parseInt(CheckpointRunner.DEFAULT_PORT)));
    }
    this.messageSerializer = msgSerializer;
  }

  public void reinitialize() {
    try {
      if (LOG.isDebugEnabled())
        LOG.debug("reinitialize(): " + getPeerName());
      this.server = RPC.getServer(this, peerAddress.getHostName(), peerAddress
          .getPort(), conf);
      server.start();
      LOG.info(" BSPPeer address:" + peerAddress.getHostName() + " port:"
          + peerAddress.getPort());
    } catch (IOException e) {
      LOG.error("Fail to start RPC server!", e);
    }

    try {
      this.zk = new ZooKeeper(quorumServers, conf.getInt(
          Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    } catch (IOException e) {
      LOG.error("Fail while reinitializing zookeeeper!", e);
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

  private String checkpointedPath() {
    String backup = conf.get("bsp.checkpoint.prefix_path", "/checkpoint/");
    String ckptPath = backup + jobConf.getJobID().toString() + "/"
        + getSuperstepCount() + "/" + this.taskid.toString();
    if (LOG.isDebugEnabled())
      LOG.debug("Messages are to be saved to " + ckptPath);
    return ckptPath;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#sync()
   */
  @Override
  public void sync() throws IOException, KeeperException, InterruptedException {
    enterBarrier();
    long startTime = System.currentTimeMillis();
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

      // checkpointing
      if (null != this.messageSerializer) {
        this.messageSerializer.serialize(new BSPSerializableMessage(
            checkpointedPath(), bundle));
      }

      peer.put(bundle);
    }

    if ((System.currentTimeMillis() - startTime) < 200) {
      Thread.sleep(200);
    }

    leaveBarrier();
    currentTaskStatus.incrementSuperstepCount();
    umbilical.incrementSuperstepCount(taskid);

    startTime = System.currentTimeMillis();
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

    // TODO: This is a quite temporary solution of HAMA-387.
    // If zk.getChildren() response is slower than 200 milliseconds,
    // BSP system will be hanged.

    // We have to consider another way to avoid this problem.
    if ((System.currentTimeMillis() - startTime) < 200) {
      Thread.sleep(200); // at least wait
    }
  }

  protected boolean enterBarrier() throws KeeperException, InterruptedException {
    LOG.debug("[" + getPeerName() + "] enter the enterbarrier: "
        + this.getSuperstepCount());
    zk.create(getNodeName(), Bytes.toBytes(this.getSuperstepCount()),
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

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
    zk.delete(getNodeName(), 0);
    while (true) {
      synchronized (mutex) {
        List<String> list = zk.getChildren(bspRoot, true);

        if (list.size() > 0) {
          mutex.wait();
        } else {
          return true;
        }
      }
    }
  }

  private String getNodeName() {
    return bspRoot + "/" + taskid.getJobID().toString() + "_" + getPeerName();
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
    this.clear();
    try {
      zk.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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
    String[] result = null;
    try {
      result = zk.getChildren("/" + jobConf.getJobID().toString(), this).toArray(new String[0]);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    return result;
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
