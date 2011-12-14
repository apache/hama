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
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.HamaConfiguration;

public class YARNBSPJob extends BSPJob {

  private static final Log LOG = LogFactory.getLog(YARNBSPJob.class);

  private static volatile int id = 0;

  private YARNBSPJobClient submitClient;
  private BSPClient client;
  private boolean submitted;
  private ApplicationReport report;
  private ClientRMProtocol applicationsManager;
  private YarnRPC rpc;

  public YARNBSPJob(HamaConfiguration conf) throws IOException {
    super(conf);
    submitClient = new YARNBSPJobClient(conf);
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    this.rpc = YarnRPC.create(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);

    this.applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, conf));
  }

  public void setMemoryUsedPerTaskInMb(int mem) {
    conf.setInt("bsp.child.mem.in.mb", mem);
  }

  public void kill() throws YarnRemoteException {
    if (submitClient != null) {
      KillApplicationRequest killRequest = Records
          .newRecord(KillApplicationRequest.class);
      killRequest.setApplicationId(submitClient.getId());
      applicationsManager.forceKillApplication(killRequest);
    }
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    RunningJob submitJobInternal = submitClient.submitJobInternal(this,
        new BSPJobID("hama_yarn", id++));
    if (submitJobInternal != null) {
      submitted = true;
      report = submitClient.getReport();
    }
  }

  @Override
  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("Starting job...");

    if (!submitted) {
      this.submit();
    }

    client = (BSPClient) RPC.waitForProxy(BSPClient.class, BSPClient.versionID,
        NetUtils.createSocketAddr(report.getHost(), report.getRpcPort()), conf);

    GetApplicationReportRequest reportRequest = Records
        .newRecord(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(submitClient.getId());

    GetApplicationReportResponse reportResponse = applicationsManager
        .getApplicationReport(reportRequest);
    ApplicationReport localReport = reportResponse.getApplicationReport();
    long clientSuperStep = 0L;
    // TODO this may cause infinite loops, we can go with our rpc client
    while (localReport.getFinalApplicationStatus() != null
        && localReport.getFinalApplicationStatus() != FinalApplicationStatus.FAILED
        && localReport.getFinalApplicationStatus() != FinalApplicationStatus.KILLED
        && localReport.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
      LOG.debug("currently in state: "
          + localReport.getFinalApplicationStatus());
      if (verbose) {
        long remoteSuperStep = client.getCurrentSuperStep().get();
        if (clientSuperStep > remoteSuperStep) {
          clientSuperStep = remoteSuperStep;
          LOG.info("Current supersteps number: " + clientSuperStep);
        }
      }
      reportResponse = applicationsManager.getApplicationReport(reportRequest);
      localReport = reportResponse.getApplicationReport();

      Thread.sleep(3000L);
    }

    if (localReport.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED) {
      LOG.info("Job succeeded!");
      return true;
    } else {
      LOG.info("Job failed with status: "
          + localReport.getFinalApplicationStatus().toString() + "!");
      return false;
    }

  }

  ClientRMProtocol getApplicationsManager() {
    return applicationsManager;
  }

}
