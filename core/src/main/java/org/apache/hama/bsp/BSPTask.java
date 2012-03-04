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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.ipc.BSPPeerProtocol;

/**
 * Base class for tasks.
 */
public final class BSPTask extends Task {

  public static final Log LOG = LogFactory.getLog(BSPTask.class);

  private BSPJob conf;
  BytesWritable split;
  String splitClass;

  // Schedule Heartbeats to GroomServer
  private ScheduledExecutorService pingService;

  /**
   * This thread is responsible for sending a heartbeat ping to GroomServer.
   */
  private static class PingGroomServer implements Runnable {

    private BSPPeerProtocol pingRPC;
    private TaskAttemptID taskId;
    private Thread bspThread;

    public PingGroomServer(BSPPeerProtocol umbilical, TaskAttemptID id) {
      pingRPC = umbilical;
      taskId = id;
      bspThread = Thread.currentThread();
    }

    @Override
    public void run() {

      boolean shouldKillSelf = false;
      try {
        if (LOG.isDebugEnabled())
          LOG.debug("Pinging at time " + Calendar.getInstance().toString());
        // if the RPC call returns false, it means that groomserver does not
        // have knowledge of this task.
        shouldKillSelf = !(pingRPC.ping(taskId) && bspThread.isAlive());
      } catch (IOException e) {
        LOG.error(new StringBuilder(
            "IOException pinging GroomServer from task - ").append(taskId), e);
        shouldKillSelf = true;
      } catch (Exception e) {
        LOG.error(new StringBuilder(
            "Exception pinging GroomServer from task - ").append(taskId), e);
        shouldKillSelf = true;
      }
      if (shouldKillSelf) {
        LOG.error("Killing self. No connection to groom.");
        System.exit(69);
      }

    }
  }

  public BSPTask() {
    this.pingService = Executors.newScheduledThreadPool(1);
  }

  public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid,
      int partition, String splitClass, BytesWritable split) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.taskId = taskid;
    this.partition = partition;

    this.splitClass = splitClass;
    this.split = split;

    this.pingService = Executors.newScheduledThreadPool(1);
  }

  @Override
  public final BSPTaskRunner createRunner(GroomServer groom) {
    return new BSPTaskRunner(this, groom, this.conf);
  }

  private void startPingingGroom(BSPJob job, BSPPeerProtocol umbilical) {

    long pingPeriod = job.getConf().getLong(Constants.GROOM_PING_PERIOD,
        Constants.DEFAULT_GROOM_PING_PERIOD) / 2;

    try {
      if (pingPeriod > 0) {
        pingService.scheduleWithFixedDelay(new PingGroomServer(umbilical,
            taskId), 0, pingPeriod, TimeUnit.MILLISECONDS);
        LOG.error("Started pinging to groom");
      }
    } catch (Exception e) {
      LOG.error("Error scheduling ping service", e);
    }
  }

  private void stopPingingGroom() {
    if (pingService != null) {
      LOG.error("Shutting down ping service.");
      pingService.shutdownNow();
    }
  }

  @Override
  public final void run(BSPJob job, BSPPeerImpl<?, ?, ?, ?, ?> bspPeer,
      BSPPeerProtocol umbilical) throws Exception {

    startPingingGroom(job, umbilical);
    try {
      runBSP(job, bspPeer, split, umbilical);
      done(umbilical);
    } finally {
      stopPingingGroom();
    }

  }

  @SuppressWarnings("unchecked")
  private final <KEYIN, VALUEIN, KEYOUT, VALUEOUT, M extends Writable> void runBSP(
      final BSPJob job,
      BSPPeerImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> bspPeer,
      final BytesWritable rawSplit, final BSPPeerProtocol umbilical)
      throws Exception {

    BSP<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> bsp = (BSP<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M>) ReflectionUtils
        .newInstance(job.getConf().getClass("bsp.work.class", BSP.class),
            job.getConf());

    // The policy is to throw the first exception and log the remaining.
    Exception firstException = null;
    try {
      bsp.setup(bspPeer);
      bsp.bsp(bspPeer);
    } catch (Exception e) {
      LOG.error("Error running bsp setup and bsp function.", e);
      firstException = e;
    } finally {
      try {
        bsp.cleanup(bspPeer);
      } catch (Exception e) {
        LOG.error("Error cleaning up after bsp executed.", e);
        if (firstException == null)
          firstException = e;
      } finally {

        try {
          bspPeer.close();
        } catch (Exception e) {
          LOG.error("Error closing BSP Peer.", e);
          if (firstException == null)
            firstException = e;
        }
        if (firstException != null)
          throw firstException;
      }
    }
  }

  public final BSPJob getConf() {
    return conf;
  }

  public final void setConf(BSPJob conf) {
    this.conf = conf;
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    super.write(out);
    if (split != null) {
      out.writeBoolean(true);
      Text.writeString(out, splitClass);
      split.write(out);
      split = null;
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (in.readBoolean()) {
      splitClass = Text.readString(in);
      if (split == null) {
        split = new BytesWritable();
      }
      split.readFields(in);
    }
  }

}
