/**
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

import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TestBSPTaskFaults.MinimalGroomServer;
import org.apache.hama.bsp.messages.ByteMessage;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.BSPNetUtils;

public class TestCheckpoint extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestCheckpoint.class);

  static final String checkpointedDir = "checkpoint/job_201110302255_0001/0/";

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testCheckpoint() throws Exception {
    Configuration config = new HamaConfiguration();
    FileSystem dfs = FileSystem.get(config);

    BSPPeerImpl bspTask = new BSPPeerImpl(config, dfs);
    bspTask.setCurrentTaskStatus(new TaskStatus(new BSPJobID(),
        new TaskAttemptID(), 1.0f, TaskStatus.State.RUNNING, "running",
        "127.0.0.1", TaskStatus.Phase.STARTING, new Counters()));
    assertNotNull("BSPPeerImpl should not be null.", bspTask);
    if (dfs.mkdirs(new Path("checkpoint"))) {
      if (dfs.mkdirs(new Path("checkpoint/job_201110302255_0001"))) {
        if (dfs.mkdirs(new Path("checkpoint/job_201110302255_0001/0")))
          ;
      }
    }
    assertTrue("Make sure directory is created.",
        dfs.exists(new Path(checkpointedDir)));
    byte[] tmpData = "data".getBytes();
    BSPMessageBundle bundle = new BSPMessageBundle();
    bundle.addMessage(new ByteMessage("abc".getBytes(), tmpData));
    assertNotNull("Message bundle can not be null.", bundle);
    assertNotNull("Configuration should not be null.", config);
    bspTask.checkpoint(checkpointedDir + "/attempt_201110302255_0001_000000_0",
        bundle);
    FSDataInputStream in = dfs.open(new Path(checkpointedDir
        + "/attempt_201110302255_0001_000000_0"));
    BSPMessageBundle bundleRead = new BSPMessageBundle();
    bundleRead.readFields(in);
    in.close();
    ByteMessage byteMsg = (ByteMessage) (bundleRead.getMessages()).get(0);
    String content = new String(byteMsg.getData());
    LOG.info("Saved checkpointed content is " + content);
    assertTrue("Message content should be the same.", "data".equals(content));
    dfs.delete(new Path("checkpoint"), true);
  }

  public void testCheckpointInterval() throws Exception {

    HamaConfiguration conf = new HamaConfiguration();

    conf.setClass(SyncServiceFactory.SYNC_CLIENT_CLASS,
        LocalBSPRunner.LocalSyncClient.class, SyncClient.class);

    conf.setBoolean(Constants.CHECKPOINT_ENABLED, false);

    int port = BSPNetUtils.getFreePort(5000);
    InetSocketAddress inetAddress = new InetSocketAddress(port);
    MinimalGroomServer groom = new MinimalGroomServer(conf);
    Server workerServer = RPC.getServer(groom, inetAddress.getHostName(),
        inetAddress.getPort(), conf);
    workerServer.start();

    LOG.info("Started RPC server");
    conf.setInt("bsp.groom.rpc.port", inetAddress.getPort());

    BSPPeerProtocol umbilical = (BSPPeerProtocol) RPC.getProxy(
        BSPPeerProtocol.class, BSPPeerProtocol.versionID, inetAddress, conf);
    LOG.info("Started the proxy connections");

    TaskAttemptID tid = new TaskAttemptID(new TaskID(new BSPJobID(
        "job_201110102255", 1), 1), 1);

    try {
      BSPJob job = new BSPJob(conf);
      final BSPPeerProtocol proto = (BSPPeerProtocol) RPC.getProxy(
          BSPPeerProtocol.class, BSPPeerProtocol.versionID,
          new InetSocketAddress("127.0.0.1", port), conf);

      BSPTask task = new BSPTask();
      task.setConf(job);

      @SuppressWarnings("rawtypes")
      BSPPeerImpl<?, ?, ?, ?, ?> bspPeer = new BSPPeerImpl(job, conf, tid,
          proto, 0, null, null, new Counters());

      bspPeer.setCurrentTaskStatus(new TaskStatus(new BSPJobID(), tid, 1.0f,
          TaskStatus.State.RUNNING, "running", "127.0.0.1",
          TaskStatus.Phase.STARTING, new Counters()));

      assertEquals(bspPeer.isReadyToCheckpoint(), false);

      conf.setBoolean(Constants.CHECKPOINT_ENABLED, true);
      conf.setInt(Constants.CHECKPOINT_INTERVAL, 3);

      bspPeer.sync();

      LOG.info("Is Ready = " + bspPeer.isReadyToCheckpoint() + " at step "
          + bspPeer.getSuperstepCount());
      assertEquals(bspPeer.isReadyToCheckpoint(), false);
      bspPeer.sync();
      LOG.info("Is Ready = " + bspPeer.isReadyToCheckpoint() + " at step "
          + bspPeer.getSuperstepCount());
      assertEquals(bspPeer.isReadyToCheckpoint(), false);
      bspPeer.sync();
      LOG.info("Is Ready = " + bspPeer.isReadyToCheckpoint() + " at step "
          + bspPeer.getSuperstepCount());
      assertEquals(bspPeer.isReadyToCheckpoint(), true);

      job.setCheckPointInterval(5);
      bspPeer.sync();
      LOG.info("Is Ready = " + bspPeer.isReadyToCheckpoint() + " at step "
          + bspPeer.getSuperstepCount());
      assertEquals(bspPeer.isReadyToCheckpoint(), false);
      bspPeer.sync();
      LOG.info("Is Ready = " + bspPeer.isReadyToCheckpoint() + " at step "
          + bspPeer.getSuperstepCount());
      assertEquals(bspPeer.isReadyToCheckpoint(), true);

    } catch (Exception e) {
      LOG.error("Error testing BSPPeer.", e);
    } finally {
      umbilical.close();
      Thread.sleep(2000);
      workerServer.stop();
      Thread.sleep(2000);
    }

  }
}
