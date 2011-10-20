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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.sync.rpc.RPCSyncClientImpl;
import org.apache.hama.bsp.sync.rpc.RPCSyncServerImpl;
import org.apache.hama.bsp.sync.zookeeper.ZooKeeperSyncClientImpl;
import org.apache.hama.bsp.sync.zookeeper.ZooKeeperSyncServerImpl;

public class TestSyncServiceFactory extends TestCase {

  public void testClientInstantiation() throws Exception {

    Configuration conf = new Configuration();
    // given null, should return zookeeper
    SyncClient syncClient = SyncServiceFactory.getSyncClient(conf);
    assertTrue(syncClient instanceof ZooKeeperSyncClientImpl);
    
    // other class
    conf.set(SyncServiceFactory.SYNC_CLIENT_CLASS, RPCSyncClientImpl.class.getCanonicalName());
    syncClient = SyncServiceFactory.getSyncClient(conf);
    assertTrue(syncClient instanceof RPCSyncClientImpl);

  }
  
  public void testServerInstantiation() throws Exception {

    Configuration conf = new Configuration();
    // given null, should return zookeeper
    SyncServer syncServer = SyncServiceFactory.getSyncServer(conf);
    assertTrue(syncServer instanceof ZooKeeperSyncServerImpl);
    
    // other class
    conf.set(SyncServiceFactory.SYNC_SERVER_CLASS, RPCSyncServerImpl.class.getCanonicalName());
    syncServer = SyncServiceFactory.getSyncServer(conf);
    assertTrue(syncServer instanceof RPCSyncServerImpl);

  }

}
