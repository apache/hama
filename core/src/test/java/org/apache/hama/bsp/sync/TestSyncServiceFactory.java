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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.util.BSPNetUtils;
import org.junit.Test;

public class TestSyncServiceFactory extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestCase.class);

  public static class ListenerTest extends ZKSyncEventListener {

    private Text value;

    public ListenerTest() {
      value = new Text("init");
    }

    public String getValue() {
      return value.toString();
    }

    @Override
    public void onDelete() {

    }

    @Override
    public void onChange() {
      LOG.info("ZK value changed event triggered.");
      value.set("Changed");

    }

    @Override
    public void onChildKeySetChange() {

    }

  }

  @Test
  public void testClientInstantiation() throws Exception {

    Configuration conf = new Configuration();
    // given null, should return zookeeper
    PeerSyncClient syncClient = SyncServiceFactory.getPeerSyncClient(conf);
    assertTrue(syncClient instanceof ZooKeeperSyncClientImpl);
  }

  @Test
  public void testServerInstantiation() throws Exception {

    Configuration conf = new Configuration();
    // given null, should return zookeeper
    SyncServer syncServer = SyncServiceFactory.getSyncServer(conf);
    assertTrue(syncServer instanceof ZooKeeperSyncServerImpl);
  }

  private static class ZKServerThread implements Runnable {

    SyncServer server;

    ZKServerThread(SyncServer s) {
      server = s;
    }

    @Override
    public void run() {
      try {
        server.start();
      } catch (Exception e) {
        LOG.error("Error running server.", e);
      }
    }

  }

  @Test
  public void testZKSyncStore() throws Exception {
    Configuration conf = new Configuration();
    int zkPort = BSPNetUtils.getFreePort(21811);
    conf.set("bsp.local.dir", "/tmp/hama-test");
    conf.set("bsp.output.dir", "/tmp/hama-test_out");
    conf.setInt(Constants.PEER_PORT, zkPort);
    conf.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    conf.setInt(Constants.ZOOKEEPER_CLIENT_PORT, zkPort);
    conf.set(Constants.ZOOKEEPER_SESSION_TIMEOUT, "12000");
    System.setProperty("user.dir", "/tmp");
    // given null, should return zookeeper
    final SyncServer syncServer = SyncServiceFactory.getSyncServer(conf);
    syncServer.init(conf);
    assertTrue(syncServer instanceof ZooKeeperSyncServerImpl);

    ZKServerThread serverThread = new ZKServerThread(syncServer);
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    executorService.submit(serverThread);

    executorService.awaitTermination(10, TimeUnit.SECONDS);

    final PeerSyncClient syncClient = SyncServiceFactory
        .getPeerSyncClient(conf);
    assertTrue(syncClient instanceof ZooKeeperSyncClientImpl);
    BSPJobID jobId = new BSPJobID("abc", 1);
    TaskAttemptID taskId = new TaskAttemptID(new TaskID(jobId, 1), 1);
    syncClient.init(conf, jobId, taskId);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          syncServer.stopServer();

        } catch (Exception e) {
          // too late to log!
        }
      }
    });

    IntWritable data = new IntWritable(5);
    syncClient.storeInformation(
        syncClient.constructKey(jobId, String.valueOf(1L), "test"), data, true,
        null);

    ListenerTest listenerTest = new ListenerTest();

    syncClient.registerListener(
        syncClient.constructKey(jobId, String.valueOf(1L), "test"),
        ZKSyncEventFactory.getValueChangeEvent(), listenerTest);

    IntWritable valueHolder = new IntWritable();
    boolean result = syncClient
        .getInformation(
            syncClient.constructKey(jobId, String.valueOf(1L), "test"),
            valueHolder);
    assertTrue(result);
    int intVal = valueHolder.get();
    assertTrue(intVal == data.get());

    data.set(6);
    syncClient.storeInformation(
        syncClient.constructKey(jobId, String.valueOf(1L), "test"), data, true,
        null);
    valueHolder = new IntWritable();
    result = syncClient
        .getInformation(
            syncClient.constructKey(jobId, String.valueOf(1L), "test"),
            valueHolder);

    assertTrue(result);
    intVal = valueHolder.get();
    assertTrue(intVal == data.get());

    Thread.sleep(5000);

    assertEquals(true, listenerTest.getValue().equals("Changed"));

    syncServer.stopServer();

  }

}
