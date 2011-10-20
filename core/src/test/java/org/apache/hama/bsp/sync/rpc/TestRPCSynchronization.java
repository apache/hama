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
package org.apache.hama.bsp.sync.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncServerRunner;
import org.apache.hama.bsp.sync.SyncServiceFactory;

public class TestRPCSynchronization extends TestCase {

  public void testSubmitJob() throws Exception {
    Configuration conf = new Configuration();
    conf.set("bsp.peers.num", "1");
    conf.set(SyncServiceFactory.SYNC_SERVER_CLASS,
        RPCSyncServerImpl.class.getCanonicalName());
    SyncServerRunner syncServer = SyncServiceFactory.getSyncServerRunner(conf);
    conf = syncServer.init(conf);

    ExecutorService pool = Executors.newFixedThreadPool(1);
    pool.submit(syncServer);

    conf.set(SyncServiceFactory.SYNC_CLIENT_CLASS,
        RPCSyncClientImpl.class.getCanonicalName());
    SyncClient syncClient = SyncServiceFactory.getSyncClient(conf);
    assertTrue(syncClient instanceof RPCSyncClientImpl);
    TaskAttemptID taskId = new TaskAttemptID("0", 0, 0, 0);
    syncClient.init(conf, taskId.getJobID(), taskId);
    syncClient.register(taskId.getJobID(), taskId, "localhost", 1255123);

    syncClient.enterBarrier(taskId.getJobID(), taskId, 0);
    syncClient.leaveBarrier(taskId.getJobID(), taskId, 0);
    
    String[] allPeerNames = syncClient.getAllPeerNames(taskId);
    assertEquals(allPeerNames.length, 1);
    assertEquals(allPeerNames[0],"localhost:1255123");
    
  }

}
