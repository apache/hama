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
import java.util.Collections;
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
              .valueOf(CheckpointRunner.DEFAULT_PORT)));
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
    Iterator<Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>>> it = this.outgoingQueues
        .entrySet().iterator();

    while (it.hasNext()) {
      Entry<InetSocketAddress, ConcurrentLinkedQueue<BSPMessage>> entry = it
          .next();

      BSPPeerInterface peer = peers.get(entry.getKey());
      if (peer == null) {
        try {
          peer = getBSPPeerConnection(entry.getKey());
        } catch (NullPointerException ne) {
          umbilical.fatalError(taskid, entry.getKey().getHostName()
              + " doesn't exists.");
        }
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

    leaveBarrier();
    currentTaskStatus.incrementSuperstepCount();
    umbilical.statusUpdate(taskid, currentTaskStatus);

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
  }

  private void createZnode(final String path) throws KeeperException,
      InterruptedException {
    createZnode(path, CreateMode.PERSISTENT);
  }

  private void createEphemeralZnode(final String path) throws KeeperException,
      InterruptedException {
    createZnode(path, CreateMode.EPHEMERAL);
  }

  private void createZnode(final String path, final CreateMode mode)
      throws KeeperException, InterruptedException {
    synchronized (zk) {
      Stat s = zk.exists(path, false);
      if (null == s) {
        try {
          zk.create(path, null, Ids.OPEN_ACL_UNSAFE, mode);
        } catch (KeeperException.NodeExistsException nee) {
          LOG.warn("Ignore because znode may be already created at " + path,
              nee);
        }
      }
    }
  }

  private class BarrierWatcher implements Watcher {
    private boolean complete = false;

    boolean isComplete() {
      return this.complete;
    }

    @Override
    public void process(WatchedEvent event) {
      this.complete = true;
      synchronized (mutex) {
        LOG.debug(">>>>>>>>>>>>>>> at superstep " + getSuperstepCount()
            + " taskid:" + taskid.toString() + " is notified.");
        mutex.notifyAll();
      }
    }
  }

  protected boolean enterBarrier() throws KeeperException, InterruptedException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("[" + getPeerName() + "] enter the enterbarrier: "
          + this.getSuperstepCount());
    }

    synchronized (zk) {
      createZnode(bspRoot);
      final String pathToJobIdZnode = bspRoot + "/"
          + taskid.getJobID().toString();
      createZnode(pathToJobIdZnode);
      final String pathToSuperstepZnode = pathToJobIdZnode + "/"
          + getSuperstepCount();
      createZnode(pathToSuperstepZnode);
      BarrierWatcher barrierWatcher = new BarrierWatcher();
      Stat readyStat = zk.exists(pathToSuperstepZnode + "/ready",
          barrierWatcher);
      zk.create(getNodeName(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

      List<String> znodes = zk.getChildren(pathToSuperstepZnode, false);
      int size = znodes.size(); // may contains ready
      boolean hasReady = znodes.contains("ready");
      if (hasReady) {
        size--;
      }

      LOG.debug("===> at superstep :" + getSuperstepCount()
          + " current znode size: " + znodes.size() + " current znodes:"
          + znodes);

      if (LOG.isDebugEnabled())
        LOG.debug("enterBarrier() znode size within " + pathToSuperstepZnode
            + " is " + znodes.size() + ". Znodes include " + znodes);

      if (size < jobConf.getNumBspTask()) {
        LOG.info("xxxx 1. At superstep: " + getSuperstepCount()
            + " which task is waiting? " + taskid.toString()
            + " stat is null? " + readyStat);
        while (!barrierWatcher.isComplete()) {
          if (!hasReady) {
            synchronized (mutex) {
              mutex.wait(1000);
            }
          }
        }
        LOG.debug("xxxx 2. at superstep: " + getSuperstepCount()
            + " after waiting ..." + taskid.toString());
      } else {
        LOG.debug("---> at superstep: " + getSuperstepCount()
            + " task that is creating /ready znode:" + taskid.toString());
        createEphemeralZnode(pathToSuperstepZnode + "/ready");
      }
    }
    return true;
  }

  protected boolean leaveBarrier() throws KeeperException, InterruptedException {
    final String pathToSuperstepZnode = bspRoot + "/"
        + taskid.getJobID().toString() + "/" + getSuperstepCount();
    while (true) {
      List<String> znodes = zk.getChildren(pathToSuperstepZnode, false);
      LOG
          .info("leaveBarrier() !!! checking znodes contnains /ready node or not: at superstep:"
              + getSuperstepCount() + " znode:" + znodes);
      if (znodes.contains("ready")) {
        znodes.remove("ready");
      }
      final int size = znodes.size();
      LOG.info("leaveBarrier() at superstep:" + getSuperstepCount()
          + " znode size: (" + size + ") znodes:" + znodes);
      if (null == znodes || znodes.isEmpty())
        return true;
      if (1 == size) {
        try {
          zk.delete(getNodeName(), 0);
        } catch (KeeperException.NoNodeException nne) {
          LOG.warn(
              "+++ (znode size is 1). Ignore because znode may disconnect.",
              nne);
        }
        return true;
      }
      Collections.sort(znodes);

      final String lowest = znodes.get(0);
      final String highest = znodes.get(size - 1);

      LOG.info("leaveBarrier() at superstep: " + getSuperstepCount()
          + " taskid:" + taskid.toString() + " lowest: " + lowest + " highest:"
          + highest);
      synchronized (mutex) {

        if (getNodeName().equals(pathToSuperstepZnode + "/" + lowest)) {
          Stat s = zk.exists(pathToSuperstepZnode + "/" + highest,
              new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (mutex) {
                    LOG.debug("leaveBarrier() at superstep: "
                        + getSuperstepCount() + " taskid:" + taskid.toString()
                        + " highest notify lowest.");
                    mutex.notifyAll();
                  }
                }
              });

          if (null != s) {
            LOG.debug("leaveBarrier(): superstep:" + getSuperstepCount()
                + " taskid:" + taskid.toString() + " wait for higest notify.");
            mutex.wait();
          }
        } else {
          Stat s1 = zk.exists(getNodeName(), false);

          if (null != s1) {
            LOG.info("leaveBarrier() znode at superstep:" + getSuperstepCount()
                + " taskid:" + taskid.toString() + " exists, so delete it.");
            try {
              zk.delete(getNodeName(), 0);
            } catch (KeeperException.NoNodeException nne) {
              LOG.warn("++++ Ignore because node may be dleted.", nne);
            }
          }

          Stat s2 = zk.exists(pathToSuperstepZnode + "/" + lowest,
              new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                  synchronized (mutex) {
                    LOG.debug("leaveBarrier() at superstep: "
                        + getSuperstepCount() + " taskid:" + taskid.toString()
                        + " lowest notify other nodes.");
                    mutex.notifyAll();
                  }
                }
              });
          if (null != s2) {
            LOG.debug("leaveBarrier(): superstep:" + getSuperstepCount()
                + " taskid:" + taskid.toString() + " wait for lowest notify.");
            mutex.wait();
          }
        }
      }
    }
  }

  private String getNodeName() {
    return bspRoot + "/" + taskid.getJobID().toString() + "/"
        + getSuperstepCount() + "/" + taskid.toString();
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

  protected BSPPeerInterface getBSPPeerConnection(InetSocketAddress addr)
      throws NullPointerException, IOException {
    BSPPeerInterface peer;
    synchronized (this.peers) {
      peer = peers.get(addr);

      int retries = 0;
      while (peer != null) {
        peer = (BSPPeerInterface) RPC.getProxy(BSPPeerInterface.class,
            BSPPeerInterface.versionID, addr, this.conf);

        retries++;
        if (retries > 10) {
          umbilical.fatalError(taskid, addr + " doesn't repond.");
        }
      }

      this.peers.put(addr, peer);
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
    return new InetSocketAddress(peerAddrParts[0], Integer
        .valueOf(peerAddrParts[1]));
  }

  @Override
  public String[] getAllPeerNames() {
    String[] result = null;
    try {
      result = zk.getChildren("/" + jobConf.getJobID().toString(), this)
          .toArray(new String[0]);
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
