/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This client class abstracts the use of our zookeeper sync code.
 * 
 */
public class ZooKeeperSyncClientImpl extends ZKSyncClient implements
    PeerSyncClient {

  /*
   * TODO maybe extract an abstract class and let the subclasses implement
   * enter-/leaveBarrier so we can have multiple implementations, just like
   * goldenorb.
   */

  public static final Log LOG = LogFactory
      .getLog(ZooKeeperSyncClientImpl.class);

  private volatile Integer mutex = 0;

  private String quorumServers;
  private ZooKeeper zk;
  private String bspRoot;
  private InetSocketAddress peerAddress;
  private int numBSPTasks;
  // allPeers is lazily initialized
  private String[] allPeers;

  @Override
  public void init(Configuration conf, BSPJobID jobId, TaskAttemptID taskId)
      throws Exception {
    quorumServers = QuorumPeer.getZKQuorumServersString(conf);
    this.zk = new ZooKeeper(quorumServers, conf.getInt(
        Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);
    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);

    initialize(this.zk, bspRoot);

    peerAddress = new InetSocketAddress(bindAddress, bindPort);
    LOG.info("Start connecting to Zookeeper! At " + peerAddress);
    numBSPTasks = conf.getInt("bsp.peers.num", 1);
  }

  @Override
  public void enterBarrier(BSPJobID jobId, TaskAttemptID taskId, long superstep)
      throws SyncException {
    LOG.debug("[" + getPeerName() + "] enter the enterbarrier: " + superstep);

    try {
      synchronized (zk) {

        final String pathToSuperstepZnode = constructKey(taskId.getJobID(),
            "sync", "" + superstep);

        writeNode(pathToSuperstepZnode, null, true, null);
        BarrierWatcher barrierWatcher = new BarrierWatcher();
        // this is really needed to register the barrier watcher, don't remove
        // this line!
        zk.exists(pathToSuperstepZnode + "/ready", barrierWatcher);
        zk.create(getNodeName(taskId, superstep), null, Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL);

        List<String> znodes = zk.getChildren(pathToSuperstepZnode, false);
        int size = znodes.size(); // may contains ready
        boolean hasReady = znodes.contains("ready");
        if (hasReady) {
          size--;
        }

        LOG.debug("===> at superstep :" + superstep + " current znode size: "
            + znodes.size() + " current znodes:" + znodes);

        LOG.debug("enterBarrier() znode size within " + pathToSuperstepZnode
            + " is " + znodes.size() + ". Znodes include " + znodes);

        if (size < numBSPTasks) {
          while (!barrierWatcher.isComplete()) {
            if (!hasReady) {
              synchronized (mutex) {
                mutex.wait(1000);
              }
            }
          }
          LOG.debug("2. at superstep: " + superstep + " after waiting ..."
              + taskId.toString());
        } else {
          LOG.debug("---> at superstep: " + superstep
              + " task that is creating /ready znode:" + taskId.toString());
          writeNode(pathToSuperstepZnode + "/ready", null, false, null);
        }
      }
    } catch (Exception e) {
      throw new SyncException(e.toString());
    }
  }

  @Override
  public void leaveBarrier(final BSPJobID jobId, final TaskAttemptID taskId,
      final long superstep) throws SyncException {
    try {
      // final String pathToSuperstepZnode = bspRoot + "/"
      // + taskId.getJobID().toString() + "/" + superstep;
      final String pathToSuperstepZnode = constructKey(taskId.getJobID(),
          "sync", "" + superstep);
      while (true) {
        List<String> znodes = zk.getChildren(pathToSuperstepZnode, false);
        LOG.debug("leaveBarrier() !!! checking znodes contnains /ready node or not: at superstep:"
            + superstep + " znode:" + znodes);
        if (znodes.contains("ready")) {
          znodes.remove("ready");
        }
        final int size = znodes.size();

        LOG.debug("leaveBarrier() at superstep:" + superstep + " znode size: ("
            + size + ") znodes:" + znodes);

        if (null == znodes || znodes.isEmpty())
          return;
        if (1 == size) {
          try {
            zk.delete(getNodeName(taskId, superstep), 0);
          } catch (KeeperException.NoNodeException nne) {
            LOG.debug(
                "+++ (znode size is 1). Ignore because znode may disconnect.",
                nne);
          }
          return;
        }
        Collections.sort(znodes);

        final String lowest = znodes.get(0);
        final String highest = znodes.get(size - 1);

        synchronized (mutex) {

          if (getNodeName(taskId, superstep).equals(
              pathToSuperstepZnode + "/" + lowest)) {
            Stat s = zk.exists(pathToSuperstepZnode + "/" + highest,
                new Watcher() {
                  @Override
                  public void process(WatchedEvent event) {
                    synchronized (mutex) {
                      LOG.debug("leaveBarrier() at superstep: " + superstep
                          + " taskid:" + taskId.toString()
                          + " highest notify lowest.");
                      mutex.notifyAll();
                    }
                  }
                });

            if (null != s) {
              LOG.debug("leaveBarrier(): superstep:" + superstep + " taskid:"
                  + taskId.toString() + " wait for higest notify.");
              mutex.wait();
            }
          } else {
            Stat s1 = zk.exists(getNodeName(taskId, superstep), false);

            if (null != s1) {
              try {
                zk.delete(getNodeName(taskId, superstep), 0);
              } catch (KeeperException.NoNodeException nne) {
                LOG.debug("++++ Ignore because node may be dleted.", nne);
              }
            }

            Stat s2 = zk.exists(pathToSuperstepZnode + "/" + lowest,
                new Watcher() {
                  @Override
                  public void process(WatchedEvent event) {
                    synchronized (mutex) {
                      LOG.debug("leaveBarrier() at superstep: " + superstep
                          + " taskid:" + taskId.toString()
                          + " lowest notify other nodes.");
                      mutex.notifyAll();
                    }
                  }
                });
            if (null != s2) {
              LOG.debug("leaveBarrier(): superstep:" + superstep + " taskid:"
                  + taskId.toString() + " wait for lowest notify.");
              mutex.wait();
            }
          }
        }
      }
    } catch (Exception e) {
      throw new SyncException(e.getMessage());
    }
  }

  @Override
  public void register(BSPJobID jobId, TaskAttemptID taskId,
      String hostAddress, long port) {
    try {
      String jobRegisterKey = constructKey(jobId, "peers");
      if (zk.exists(jobRegisterKey, false) == null) {
        zk.create(jobRegisterKey, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
    registerTask(jobId, hostAddress, port, taskId);
  }

  /**
   * Registers the task from outside, most of the time used by the groom which
   * uses this at task spawn-time.
   * 
   * @param zk
   * @param jobId
   * @param taskId
   * @param hostAddress
   * @param port
   * @param taskId
   */
  public void registerTask(BSPJobID jobId, String hostAddress, long port,
      TaskAttemptID taskId) {

    // byte[] taskIdBytes = serializeTaskId(taskId);
    String taskRegisterKey = constructKey(jobId, "peers", hostAddress + ":"
        + port);
    writeNode(taskRegisterKey, taskId, false, null);

  }

  @Override
  public String[] getAllPeerNames(TaskAttemptID taskId) {
    if (allPeers == null) {
      TreeMap<Integer, String> sortedMap = new TreeMap<Integer, String>();
      try {
        allPeers = zk.getChildren(constructKey(taskId.getJobID(), "peers"),
            this).toArray(new String[0]);

        for (String s : allPeers) {
          byte[] data = zk.getData(constructKey(taskId.getJobID(), "peers", s),
              this, null);
          TaskAttemptID thatTask = new TaskAttemptID();
          boolean result = getValueFromBytes(data, thatTask);

          if (result) {
            LOG.debug("TASK mapping from zookeeper: " + thatTask + " ID:"
                + thatTask.getTaskID().getId() + " : " + s);
            sortedMap.put(thatTask.getTaskID().getId(), s);
          }

        }

      } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException("All peer names could not be retrieved!");
      }

      allPeers = new String[sortedMap.size()];
      int count = 0;
      for (Entry<Integer, String> entry : sortedMap.entrySet()) {
        allPeers[count++] = entry.getValue();
        LOG.debug("TASK mapping from zookeeper: " + entry.getKey() + " : "
            + entry.getValue() + " at index " + (count - 1));
      }

    }
    return allPeers;
  }

  @Override
  public void close() throws IOException {
    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId,
      String hostAddress, long port) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopServer() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void process(WatchedEvent event) {
    synchronized (mutex) {
      mutex.notify();
    }
  }

  /*
   * UTILITY METHODS
   */

  /**
   * @return the string as host:port of this Peer
   */
  public String getPeerName() {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  /*
   * INNER CLASSES
   */

  private class BarrierWatcher implements Watcher {
    private boolean complete = false;

    boolean isComplete() {
      return this.complete;
    }

    @Override
    public void process(WatchedEvent event) {
      this.complete = true;
      synchronized (mutex) {
        mutex.notifyAll();
      }
    }
  }

}
