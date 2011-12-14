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

import javax.management.InstanceNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.sync.SyncServer;
import org.apache.hama.util.BSPNetUtils;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.hama.zookeeper.QuorumPeer.ShutdownableZooKeeperServerMain;
import org.apache.hama.zookeeper.QuorumPeer.ZookeeperTuple;

/**
 * 
 * Launches a new ZooKeeper. Basically just used by YARN.
 * 
 */
public class ZooKeeperSyncServerImpl implements SyncServer {

  private final Log LOG = LogFactory.getLog(ZooKeeperSyncServerImpl.class);
  private Configuration conf;
  private ShutdownableZooKeeperServerMain zooKeeper;

  @Override
  public Configuration init(Configuration conf) throws Exception {
    this.conf = conf;
    int port = BSPNetUtils.getFreePort(15600);

    String portString = conf.get("hama.zookeeper.property.clientPort");
    if (portString != null) {
      port = Integer.parseInt(portString);
    }

    String host = BSPNetUtils.getCanonicalHostname();
    String hostConfigured = conf.get("hama.zookeeper.quorum");
    if (hostConfigured != null) {
      // since someone can set multiple quorums via comma separated string, we
      // split on comma and just use the first one. The client itself just uses
      // the "hama.zookeeper.quorum" property.
      host = hostConfigured.split(",")[0];
    }

    if (conf.get("hama.zookeeper.property.dataDir") == null) {
      conf.set("hama.zookeeper.property.dataDir", conf.get("bsp.local.dir")
          + "zookeeper");
    }

    // always set them
    conf.set("hama.zookeeper.quorum", host);
    conf.set("hama.zookeeper.property.clientPort", port + "");

    conf.set("hama.sync.server.address", host + ":" + port);

    return conf;
  }

  @Override
  public void start() throws Exception {
    try {
      ZookeeperTuple tuple = QuorumPeer.runShutdownableZooKeeper(conf);
      zooKeeper = tuple.main;
      zooKeeper.runFromConfig(tuple.conf);
    } catch (InstanceNotFoundException e) {
      LOG.debug(e);
    }
  }

  @Override
  public void stopServer() {
    try {
      zooKeeper.shutdownZookeeperMain();
    } catch (Throwable e) {
      LOG.debug(e);
    }
  }

}
