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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class SyncServiceFactory {
  public static final String SYNC_SERVER_CLASS = "hama.sync.server.class";
  public static final String SYNC_CLIENT_CLASS = "hama.sync.client.class";

  /**
   * Returns a sync client via reflection based on what was configured.
   * 
   * @param conf
   * @return
   */
  public static SyncClient getSyncClient(Configuration conf)
      throws ClassNotFoundException {
    return (SyncClient) ReflectionUtils.newInstance(conf.getClassByName(conf
        .get(SYNC_CLIENT_CLASS,
            "org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl")), conf);
  }

  /**
   * Returns a sync server via reflection based on what was configured.
   * 
   * @param conf
   * @return
   */
  public static SyncServer getSyncServer(Configuration conf)
      throws ClassNotFoundException {
    return (SyncServer) ReflectionUtils.newInstance(conf.getClassByName(conf
        .get(SYNC_SERVER_CLASS,
            "org.apache.hama.bsp.sync.ZooKeeperSyncServerImpl")), conf);
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
