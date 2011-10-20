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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.sync.zookeeper.ZooKeeperSyncClientImpl;
import org.apache.hama.bsp.sync.zookeeper.ZooKeeperSyncServerImpl;

public class SyncServiceFactory {

  private static final Log LOG = LogFactory.getLog(SyncServiceFactory.class);

  public static final String SYNC_SERVER_CLASS = "hama.sync.server.class";
  public static final String SYNC_CLIENT_CLASS = "hama.sync.client.class";

  /**
   * Returns a sync client via reflection based on what was configured.
   * 
   * @param conf
   * @return
   */
  public static SyncClient getSyncClient(Configuration conf) {
    if (conf.get(SYNC_CLIENT_CLASS) != null) {
      try {
        return (SyncClient) ReflectionUtils.newInstance(
            conf.getClassByName(conf.get(SYNC_CLIENT_CLASS)), conf);
      } catch (ClassNotFoundException e) {
        LOG.error(
            "Class for sync client has not been found, returning default zookeeper client!",
            e);
      }
    } else {
      LOG.info("No property set for \"hama.sync.client.class\", using default zookeeper client");
    }
    return ReflectionUtils.newInstance(ZooKeeperSyncClientImpl.class, conf);
  }

  /**
   * Returns a sync server via reflection based on what was configured.
   * 
   * @param conf
   * @return
   */
  public static SyncServer getSyncServer(Configuration conf) {
    if (conf.get(SYNC_SERVER_CLASS) != null) {
      try {
        return (SyncServer) ReflectionUtils.newInstance(
            conf.getClassByName(conf.get(SYNC_SERVER_CLASS)), conf);
      } catch (ClassNotFoundException e) {
        LOG.error(
            "Class for sync server has not been found, returning default zookeeper server!",
            e);
      }
    } else {
      LOG.info("No property set for \"hama.sync.server.class\", using default zookeeper client");
    }
    return ReflectionUtils.newInstance(ZooKeeperSyncServerImpl.class, conf);
  }

  /**
   * Returns a sync server runner via reflection based on what was configured.
   * 
   * @param conf
   * @return
   */
  public static SyncServerRunner getSyncServerRunner(Configuration conf) {
    return new SyncServerRunner(conf);
  }

}
