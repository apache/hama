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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper sunchronization client that is used by BSPMaster to maintain global
 * state of cluster.
 */
public class ZKSyncBSPMasterClient extends ZKSyncClient implements
    MasterSyncClient {

  private ZooKeeper zk = null;
  private String bspRoot = null;

  Log LOG = LogFactory.getLog(ZKSyncBSPMasterClient.class);

  @Override
  public void init(HamaConfiguration conf) {
    try {
      zk = new ZooKeeper(QuorumPeer.getZKQuorumServersString(conf),
          conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    } catch (IOException e) {
      LOG.error("Exception during reinitialization!", e);
    }

    bspRoot = conf.get(Constants.ZOOKEEPER_ROOT,
        Constants.DEFAULT_ZOOKEEPER_ROOT);
    LOG.info("Initialized ZK " + (null == zk));
    Stat s = null;
    if (zk != null) {
      initialize(zk, bspRoot);
      try {
        s = zk.exists(bspRoot, false);
      } catch (Exception e) {
        LOG.error(s, e);
      }

      if (s == null) {
        try {
          zk.create(bspRoot, new byte[0], Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          LOG.error(e);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      } else {
        this.clearZKNodes();
      }
    }

  }

  private void createJobRoot(String string) {
    writeNode(string, null, true, null);
  }

  @Override
  public void clear() {
    clearZKNodes();
  }

  @Override
  public void registerJob(String string) {
    // TODO: delete job root if key is present.
    createJobRoot(string);
  }

  @Override
  public void deregisterJob(String string) {
    try {
      clearZKNodes(bspRoot + "/" + string);
      this.zk.delete(bspRoot + "/" + string, -1);
    } catch (KeeperException e) {
      LOG.error("Error deleting job " + string);
    } catch (InterruptedException e) {
      LOG.error("Error deleting job " + string);
    }

  }

  @Override
  public void close() {
    try {
      this.zk.close();
    } catch (InterruptedException e) {
      LOG.error("Error closing sync client", e);
    }

  }

  @Override
  public void process(WatchedEvent arg0) {
    LOG.debug("Processing event " + arg0.getPath());
    LOG.debug("Processing event type " + arg0.getType().toString());

  }

  public ZooKeeper getZK() {
    return this.zk;
  }

}
