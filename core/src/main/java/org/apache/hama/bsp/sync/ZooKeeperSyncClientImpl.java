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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
public final class ZooKeeperSyncClientImpl implements SyncClient, Watcher {

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
  public final void init(Configuration conf, BSPJobID jobId,
      TaskAttemptID taskId) throws Exception {
    quorumServers = QuorumPeer.getZKQuorumServersString(conf);
    this.zk = new ZooKeeper(quorumServers, conf.getInt(
        Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);
    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);

    peerAddress = new InetSocketAddress(bindAddress, bindPort);
    numBSPTasks = conf.getInt("bsp.peers.num", 1);
  }

  @Override
  public final void enterBarrier(BSPJobID jobId, TaskAttemptID taskId,
      long superstep) throws SyncException {
    LOG.debug("[" + getPeerName() + "] enter the enterbarrier: " + superstep);

    try {
      synchronized (zk) {
        createZnode(bspRoot);
        final String pathToJobIdZnode = bspRoot + "/"
            + taskId.getJobID().toString();
        createZnode(pathToJobIdZnode);
        final String pathToSuperstepZnode = pathToJobIdZnode + "/" + superstep;
        createZnode(pathToSuperstepZnode);
        BarrierWatcher barrierWatcher = new BarrierWatcher();

        // TODO not used?
        // Stat readyStat = zk.exists(pathToSuperstepZnode + "/ready",
        // barrierWatcher);
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
          createEphemeralZnode(pathToSuperstepZnode + "/ready");
        }
      }
    } catch (Exception e) {
      throw new SyncException(e.toString());
    }
  }

  @Override
  public final void leaveBarrier(final BSPJobID jobId,
      final TaskAttemptID taskId, final long superstep) throws SyncException {
    try {
      final String pathToSuperstepZnode = bspRoot + "/"
          + taskId.getJobID().toString() + "/" + superstep;
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
            LOG.warn(
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
                LOG.warn("++++ Ignore because node may be dleted.", nne);
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
      throw new SyncException(e.toString());
    }
  }

  @Override
  public final void register(BSPJobID jobId, TaskAttemptID taskId,
      String hostAddress, long port) {
    try {
      if (zk.exists("/" + jobId.toString(), false) == null) {
        zk.create("/" + jobId.toString(), new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
    registerTask(zk, jobId, hostAddress, port, taskId);
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
  public final static void registerTask(ZooKeeper zk, BSPJobID jobId,
      String hostAddress, long port, TaskAttemptID taskId) {

    byte[] taskIdBytes = serializeTaskId(taskId);

    try {
      zk.create("/" + jobId.toString() + "/" + hostAddress + ":" + port,
          taskIdBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
  }

  private final static byte[] serializeTaskId(TaskAttemptID taskId) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    try {
      taskId.write(out);
    } catch (IOException e) {
      LOG.error(e);
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
    return bos.toByteArray();
  }

  public final static TaskAttemptID deserializeTaskId(byte[] arr) {
    ByteArrayInputStream bis = new ByteArrayInputStream(arr);
    DataInputStream in = new DataInputStream(bis);
    TaskAttemptID id = new TaskAttemptID();
    try {
      id.readFields(in);
    } catch (IOException e) {
      LOG.error(e);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.error(e);
      }
    }
    return id;
  }

  @Override
  public final String[] getAllPeerNames(TaskAttemptID taskId) {
    if (allPeers == null) {
      TreeMap<Integer, String> sortedMap = new TreeMap<Integer, String>();
      try {
        allPeers = zk.getChildren("/" + taskId.getJobID().toString(), this)
            .toArray(new String[0]);

        for (String s : allPeers) {
          byte[] data = zk.getData(
              "/" + taskId.getJobID().toString() + "/" + s, this, null);
          TaskAttemptID thatTask = deserializeTaskId(data);
          LOG.debug("TASK mapping from zookeeper: " + thatTask + " ID:"
              + thatTask.getTaskID().getId() + " : " + s);
          sortedMap.put(thatTask.getTaskID().getId(), s);
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
  public final void close() throws InterruptedException {
    zk.close();
  }

  @Override
  public final void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId,
      String hostAddress, long port) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public final void stopServer() {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public final void process(WatchedEvent event) {
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
  public final String getPeerName() {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  private final String getNodeName(TaskAttemptID taskId, long superstep) {
    return bspRoot + "/" + taskId.getJobID().toString() + "/" + superstep + "/"
        + taskId.toString();
  }

  private final void createZnode(final String path) throws KeeperException,
      InterruptedException {
    createZnode(path, CreateMode.PERSISTENT);
  }

  private final void createEphemeralZnode(final String path)
      throws KeeperException, InterruptedException {
    createZnode(path, CreateMode.EPHEMERAL);
  }

  private final void createZnode(final String path, final CreateMode mode)
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
