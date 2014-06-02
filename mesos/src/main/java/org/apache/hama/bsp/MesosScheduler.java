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
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class MesosScheduler extends TaskWorkerManager implements Scheduler {
  public static final Log LOG = LogFactory.getLog(MesosScheduler.class);

  private ResourceManager resourceManager;

  SchedulerDriver driver;

  public MesosScheduler() {
  }

  @Override
  public void init(AtomicReference<GroomServerManager> groomServerManager,
      Configuration conf) {
    super.init(groomServerManager, conf);
    start();
    resourceManager = new ResourceManager(conf, groomServerManager, driver);
  }

  private void start() {
    try {
      FrameworkInfo frameworkInfo = FrameworkInfo.newBuilder()
          .setUser("")
          // Let Mesos fill in the user.
          .setCheckpoint(conf.getBoolean("bsp.master.port", false))
          .setRole(conf.get("hama.mesos.role", "*"))
          .setName(
              "Hama: (Master Port: " + conf.get("bsp.groom.rpc.port") + ","
                  + " WebUI port: " + conf.get("bsp.http.groomserver.port")
                  + ")").build();

      String master = conf.get("hama.mesos.master", "local");
      driver = new MesosSchedulerDriver(this, frameworkInfo, master);
      driver.start();
    } catch (Exception e) {
      // If the MesosScheduler can't be loaded, the JobTracker won't be useful
      // at all, so crash it now so that the user notices.
      LOG.fatal("Failed to start MesosScheduler", e);
      System.exit(1);
    }
  }

  @Override
  public synchronized void disconnected(SchedulerDriver schedulerDriver) {
    LOG.warn("Disconnected from Mesos master.");
  }

  @Override
  public synchronized void error(SchedulerDriver schedulerDriver, String s) {
    LOG.error("Error from scheduler driver: " + s);
  }

  @Override
  public synchronized void executorLost(SchedulerDriver schedulerDriver,
      ExecutorID executorID, SlaveID slaveID, int status) {
    LOG.warn("Executor " + executorID.getValue() + " lost with status "
        + status + " on slave " + slaveID);
  }

  @Override
  public synchronized void frameworkMessage(SchedulerDriver schedulerDriver,
      ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
    LOG.info("Framework Message of " + bytes.length + " bytes"
        + " from executor " + executorID.getValue() + " on slave "
        + slaveID.getValue());
  }

  @Override
  public synchronized void offerRescinded(SchedulerDriver schedulerDriver,
      OfferID offerID) {
    LOG.warn("Rescinded offer: " + offerID.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameID,
      MasterInfo masterInfo) {
    LOG.info("Registered as " + frameID.getValue() + " with master "
        + masterInfo);
  }

  @Override
  public void reregistered(SchedulerDriver arg0, MasterInfo masterInfo) {
    LOG.info("Re-registered with master " + masterInfo);
  }

  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offers) {
    resourceManager.resourceOffers(schedulerDriver, offers);
  }

  @Override
  public synchronized void slaveLost(SchedulerDriver schedulerDriver,
      SlaveID slaveID) {
    LOG.warn("Slave lost: " + slaveID.getValue());
  }

  @Override
  public synchronized void statusUpdate(SchedulerDriver schedulerDriver,
      Protos.TaskStatus taskStatus) {
    LOG.info("Status update of " + taskStatus.getTaskId().getValue() + " to "
        + taskStatus.getState().name() + " with message "
        + taskStatus.getMessage());

    switch (taskStatus.getState()) {
    case TASK_FINISHED:
    case TASK_FAILED:
    case TASK_KILLED:
    case TASK_LOST:
    case TASK_STAGING:
    case TASK_STARTING:
    case TASK_RUNNING:
      break;
    default:
      LOG.error("Unexpected TaskStatus: " + taskStatus.getState().name());
      break;
    }
  }

  @Override
  public TaskWorker spawnWorker(JobInProgress jip) {
    LOG.info("Spawning worker for: " + jip);
    return resourceManager.new MesosTaskWorker(jip);
  }

}
