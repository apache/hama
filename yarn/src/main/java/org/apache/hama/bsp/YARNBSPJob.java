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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;

public class YARNBSPJob extends BSPJob {

  private static final Log LOG = LogFactory.getLog(YARNBSPJob.class);

  private static volatile int id = 0;

  private YARNBSPJobClient submitClient;
  private boolean submitted;
  private ApplicationReport report;
  private ApplicationClientProtocol applicationsManager;
  private YarnRPC rpc;

  public YARNBSPJob(HamaConfiguration conf) throws IOException {
    submitClient = new YARNBSPJobClient(conf);
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    this.rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);

    this.applicationsManager = ((ApplicationClientProtocol) rpc.getProxy(
        ApplicationClientProtocol.class, rmAddress, conf));
  }

  public void setMemoryUsedPerTaskInMb(int mem) {
    conf.setInt("bsp.child.mem.in.mb", mem);
  }

  public void kill() throws YarnException, IOException {
    if (submitClient != null) {
      KillApplicationRequest killRequest = Records
          .newRecord(KillApplicationRequest.class);
      killRequest.setApplicationId(submitClient.getId());
      applicationsManager.forceKillApplication(killRequest);
    }
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    // If Constants.MAX_TASKS_PER_JOB is null, calculates the max tasks based on resource status.
    this.getConfiguration().setInt(Constants.MAX_TASKS_PER_JOB, getMaxTasks());

    LOG.debug("MaxTasks: " + this.getConfiguration().get(Constants.MAX_TASKS_PER_JOB));
    
    RunningJob submitJobInternal = submitClient.submitJobInternal(this,
        new BSPJobID("hama_yarn", id++));

    if (submitJobInternal != null) {
      submitted = true;
      report = submitClient.getReport();
    }
  }

  private int getMaxTasks() {
    int maxMem = this.getConfiguration().getInt(
        "yarn.nodemanager.resource.memory-mb", 0);
    int minAllocationMem = this.getConfiguration().getInt(
        "yarn.scheduler.minimum-allocation-mb", 1024);
    return maxMem / minAllocationMem;
  }

  @Override
  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("Starting job...");

    if (!submitted) {
      this.submit();
    }

    if (report != null && report.getApplicationId() == submitClient.getId()) {
      return true;
    } else {
      return false;
    }
  }

  ApplicationClientProtocol getApplicationsManager() {
    return applicationsManager;
  }

}
