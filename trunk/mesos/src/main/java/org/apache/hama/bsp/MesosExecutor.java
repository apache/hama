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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hama.HamaConfiguration;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;

import java.io.*;

public class MesosExecutor implements Executor {
  public static final Log LOG = LogFactory.getLog(MesosExecutor.class);
  private GroomServer groomServer;

  public static void main(String[] args) {
    MesosExecutorDriver driver = new MesosExecutorDriver(new MesosExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  private HamaConfiguration configure(final TaskInfo task) {
    Configuration conf = new Configuration(false);
    try {
      byte[] bytes = task.getData().toByteArray();
      conf.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
    } catch (IOException e) {
      LOG.warn("Failed to deserialize configuraiton.", e);
      System.exit(1);
    }

    // Set the local directories inside the executor sandbox, so that
    // different Grooms on the same host do not step on each other.
    conf.set("bsp.local.dir", System.getProperty("user.dir") + "/bsp/local");
    conf.set("bsp.tmp.dir", System.getProperty("user.dir") + "/bsp/tmp");
    conf.set("bsp.disk.queue.dir", System.getProperty("user.dir") + "/bsp/diskQueue");
    conf.set("hama.disk.vertices.path", System.getProperty("user.dir") + "/bsp/graph");

    return new HamaConfiguration(conf);
  }

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    LOG.info("Executor registered with the slave");
  }

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    LOG.info("Launching task : " + task.getTaskId().getValue());

    // Get configuration from task data (prepared by the JobTracker).
    HamaConfiguration conf = configure(task);

    Thread.currentThread().setContextClassLoader(
        FileSystem.class.getClassLoader());

    try {
      groomServer = new GroomServer(conf);
    } catch (IOException e) {
      LOG.fatal("Failed to start GroomServer", e);
      System.exit(1);
    }

    // Spin up a Groom Server in a new thread.
    new Thread("GroomServer Run Thread") {
      @Override
      public void run() {
        try {
          groomServer.run();

          // Send a TASK_FINISHED status update.
          // We do this here because we want to send it in a separate thread
          // than was used to call killTask().
          driver.sendStatusUpdate(TaskStatus.newBuilder()
              .setTaskId(task.getTaskId()).setState(TaskState.TASK_FINISHED)
              .build());

          // Give some time for the update to reach the slave.
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            LOG.error("Failed to sleep TaskTracker thread", e);
          }

          // Stop the executor.
          driver.stop();
        } catch (Throwable t) {
          LOG.error("Caught exception, committing suicide.", t);
          driver.stop();
          System.exit(1);
        }
      }
    }.start();

    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId())
        .setState(TaskState.TASK_RUNNING).build());
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    LOG.info("Killing task : " + taskId.getValue());
    try {
      groomServer.shutdown();
    } catch (IOException e) {
      LOG.error("Failed to shutdown TaskTracker", e);
    }
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    LOG.info("Executor reregistered with the slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    LOG.info("Executor disconnected from the slave");
  }

  @Override
  public void frameworkMessage(ExecutorDriver d, byte[] msg) {
    LOG.info("Executor received framework message of length: " + msg.length
        + " bytes");
  }

  @Override
  public void error(ExecutorDriver d, String message) {
    LOG.error("MesosExecutor.error: " + message);
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    LOG.info("Executor asked to shutdown");
  }
}
