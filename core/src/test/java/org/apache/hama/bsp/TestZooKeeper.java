/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.bsp.sync.ZKSyncBSPMasterClient;
import org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl;
import org.apache.hama.bsp.sync.ZooKeeperSyncServerImpl;
import org.apache.hama.util.BSPNetUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestZooKeeper extends TestCase {

  private HamaConfiguration configuration;

  public TestZooKeeper() {
    configuration = new HamaConfiguration();
    System.setProperty("user.dir", "/tmp");
    configuration.set("bsp.master.address", "localhost");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.set("bsp.local.dir", "/tmp/hama-test");
    configuration.set("bsp.output.dir", "/tmp/hama-test_out");
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT,
        BSPNetUtils.getFreePort(20000));
    configuration.set("hama.sync.client.class",
        org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl.class
            .getCanonicalName());
  }

  @Test
  public void testClearZKNodes() throws IOException, KeeperException,
      InterruptedException {
    final ZooKeeperSyncServerImpl server = new ZooKeeperSyncServerImpl();
    boolean done = false;
    try {
      server.init(configuration);
      ExecutorService executorService = Executors.newCachedThreadPool();
      executorService.submit(new Runnable() {
        @Override
        public void run() {
            try {
                server.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
      });

      executorService.awaitTermination(10, TimeUnit.SECONDS);

      String bspRoot = "/bsp";

      ZooKeeperSyncClientImpl peerClient = (ZooKeeperSyncClientImpl) SyncServiceFactory
          .getPeerSyncClient(configuration);

      ZKSyncBSPMasterClient masterClient = (ZKSyncBSPMasterClient) SyncServiceFactory
          .getMasterSyncClient(configuration);

      masterClient.init(configuration);

      Thread.sleep(100);

      Log.info("Created master and client sync clients");

      assertTrue(masterClient.hasKey(bspRoot));

      Log.info("BSP root exists");

      BSPJobID jobID = new BSPJobID("test1", 1);
      masterClient.registerJob(jobID.toString());
      TaskID taskId1 = new TaskID(jobID, 1);
      TaskID taskId2 = new TaskID(jobID, 2);

      TaskAttemptID task1 = new TaskAttemptID(taskId1, 1);
      TaskAttemptID task2 = new TaskAttemptID(taskId2, 1);

      int zkPort = BSPNetUtils.getFreePort(21815);
      configuration.setInt(Constants.PEER_PORT, zkPort);
      peerClient.init(configuration, jobID, task1);

      peerClient.registerTask(jobID, "hamanode1", 5000L, task1);
      peerClient.registerTask(jobID, "hamanode2", 5000L, task2);

      peerClient.storeInformation(
          peerClient.constructKey(jobID, "info", "level2"), new IntWritable(5),
          true, null);

      String[] names = peerClient.getAllPeerNames(task1);

      Log.info("Found child count = " + names.length);

      assertEquals(2, names.length);

      Log.info("Passed the child count test");

      masterClient.addKey(masterClient.constructKey(jobID, "peer", "1"),
          true, null);
      masterClient.addKey(masterClient.constructKey(jobID, "peer", "2"),
          true, null);

      String[] peerChild = masterClient.getChildKeySet(
          masterClient.constructKey(jobID, "peer"), null);
      Log.info("Found child count = " + peerChild.length);

      assertEquals(2, peerChild.length);

      Log.info(" Peer name " + peerChild[0]);
      Log.info(" Peer name " + peerChild[1]);

      Log.info("Passed the child key set test");

      masterClient.deregisterJob(jobID.toString());
      Log.info(masterClient.constructKey(jobID));

      Thread.sleep(200);

      assertEquals(false, masterClient.hasKey(masterClient.constructKey(jobID)));

      Log.info("Passed the key presence test");

      boolean result = masterClient
          .getInformation(masterClient.constructKey(jobID, "info", "level3"),
              new IntWritable());

      assertEquals(false, result);
      
      Writable[] writableArr = new Writable[2];
      writableArr[0] = new LongWritable(3L);
      writableArr[1] = new LongWritable(5L);
      ArrayWritable arrWritable = new ArrayWritable(LongWritable.class);
      arrWritable.set(writableArr);
      masterClient.storeInformation(
          masterClient.constructKey(jobID, "info", "level3"), 
          arrWritable, true, null);
      
      ArrayWritable valueHolder = new ArrayWritable(LongWritable.class);
      
      boolean getResult = masterClient.getInformation(
          masterClient.constructKey(jobID, "info", "level3"), valueHolder);
      
      assertTrue(getResult);
      
      assertEquals(arrWritable.get()[0], valueHolder.get()[0]);
      assertEquals(arrWritable.get()[1], valueHolder.get()[1]);
      
      Log.info("Passed array writable test");
      done = true;

    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      server.stopServer();
    }
    assertEquals(true, done);
  }

}
