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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Job.JobState;
import org.apache.hama.bsp.sync.SyncServer;
import org.apache.hama.bsp.sync.SyncServerImpl;
import org.apache.mina.util.AvailablePortFinder;

/**
 * BSPApplicationMaster is an application master for Apache Hamas BSP Engine.
 */
public class BSPApplicationMaster implements BSPClient {

  private static final Log LOG = LogFactory.getLog(BSPApplicationMaster.class);
  private static final ExecutorService threadPool = Executors
      .newFixedThreadPool(1);

  private Configuration localConf;
  private Configuration jobConf;
  private String jobFile;

  private Clock clock;
  private YarnRPC yarnRPC;
  private AMRMProtocol amrmRPC;

  private ApplicationAttemptId appAttemptId;
  private String applicationName;
  private long startTime;

  private JobImpl job;
  private BSPJobID jobId;

  private SyncServerImpl syncServer;
  private Future<Long> syncServerFuture;

  // RPC info where the AM receive client side requests
  private String hostname;
  private int clientPort;

  private Server clientServer;

  private BSPApplicationMaster(String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException();
    }

    jobFile = args[0];
    localConf = new YarnConfiguration();
    jobConf = getSubmitConfiguration(jobFile);

    applicationName = jobConf.get("bsp.job.name", "<no bsp job name defined>");
    if (applicationName.isEmpty()) {
      applicationName = "<no bsp job name defined>";
    }

    appAttemptId = getApplicationAttemptId();

    yarnRPC = YarnRPC.create(localConf);
    clock = new SystemClock();
    startTime = clock.getTime();

    jobId = new BSPJobID(appAttemptId.toString(), 0);

    // TODO this is not localhost, is it?
    hostname = InetAddress.getLocalHost().getCanonicalHostName();
    startSyncServer();
    clientPort = getFreePort();
    // TODO should have a configurable amount of RPC handlers
    this.clientServer = RPC.getServer(this, hostname, clientPort, 10, false,
        jobConf);

    /*
     * Make sure that this executes after the start of the sync server, because
     * we are readjusting the configuration.
     */
    rewriteSubmitConfiguration(jobFile, jobConf);

    amrmRPC = getYarnRPCConnection(localConf);
    registerApplicationMaster(amrmRPC, appAttemptId, hostname, clientPort, null);
  }

  /**
   * This method starts the sync server on a specific port and waits for it to
   * come up. Be aware that this method adds the "bsp.sync.server.address" that
   * is needed for a task to connect to the service.
   * 
   * @throws IOException
   */
  private void startSyncServer() throws IOException {
    int syncPort = getFreePort(15000);
    syncServer = new SyncServerImpl(jobConf.getInt("bsp.peers.num", 1),
        hostname, syncPort);
    syncServerFuture = threadPool.submit(syncServer);
    // wait for the RPC to come up
    InetSocketAddress syncAddress = NetUtils.createSocketAddr(hostname + ":"
        + syncPort);
    LOG.info("Waiting for the Sync Master at " + syncAddress);
    RPC.waitForProxy(SyncServer.class, SyncServer.versionID, syncAddress,
        jobConf);
    jobConf.set("hama.sync.server.address", hostname + ":" + syncPort);
  }

  /**
   * Connects to the Resource Manager.
   * 
   * @param yarnConf
   * @return a new RPC connection to the Resource Manager.
   */
  private AMRMProtocol getYarnRPCConnection(Configuration yarnConf) {
    // Connect to the Scheduler of the ResourceManager.
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return (AMRMProtocol) yarnRPC.getProxy(AMRMProtocol.class, rmAddress,
        yarnConf);
  }

  /**
   * Registers this application master with the Resource Manager and retrieves a
   * response which is used to launch additional containers.
   * 
   * @throws YarnRemoteException
   */
  private RegisterApplicationMasterResponse registerApplicationMaster(
      AMRMProtocol resourceManager, ApplicationAttemptId appAttemptID,
      String appMasterHostName, int appMasterRpcPort,
      String appMasterTrackingUrl) throws YarnRemoteException {

    RegisterApplicationMasterRequest appMasterRequest = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    appMasterRequest.setApplicationAttemptId(appAttemptID);
    appMasterRequest.setHost(appMasterHostName);
    appMasterRequest.setRpcPort(appMasterRpcPort);
    // TODO tracking URL
    // appMasterRequest.setTrackingUrl(appMasterTrackingUrl);
    RegisterApplicationMasterResponse response = resourceManager
        .registerApplicationMaster(appMasterRequest);
    LOG.debug("ApplicationMaster has maximum resource capability of: "
        + response.getMaximumResourceCapability().getMemory());
    return response;
  }

  /**
   * Gets the application attempt ID from the environment. This should be set by
   * YARN when the container has been launched.
   * 
   * @return a new ApplicationAttemptId which is unique and identifies this
   *         task.
   */
  private ApplicationAttemptId getApplicationAttemptId() throws IOException {
    Map<String, String> envs = System.getenv();
    if (!envs.containsKey(ApplicationConstants.APPLICATION_ATTEMPT_ID_ENV)) {
      throw new IllegalArgumentException(
          "ApplicationAttemptId not set in the environment");
    }
    return ConverterUtils.toApplicationAttemptId(envs
        .get(ApplicationConstants.APPLICATION_ATTEMPT_ID_ENV));
  }

  private void start() throws Exception {
    JobState finalState = null;
    try {
      job = new JobImpl(appAttemptId, jobConf, yarnRPC, amrmRPC, jobFile, jobId);
      finalState = job.startJob();
    } finally {
      if (this.syncServer != null) {
        this.syncServer.stopServer();
      }
      if (finalState != null) {
        LOG.info("Job \"" + applicationName + "\"'s state after completion: "
            + finalState.toString());
        LOG.info("Made " + (syncServerFuture.get() - 1L) + " supersteps!");
        LOG.info("Job took " + ((clock.getTime() - startTime) / 1000L)
            + "s to finish!");
      }
      job.cleanup();
    }
  }

  private void cleanup() throws YarnRemoteException {
    if (this.syncServer != null) {
      this.syncServer.stopServer();
    }
    FinishApplicationMasterRequest finishReq = Records
        .newRecord(FinishApplicationMasterRequest.class);
    finishReq.setAppAttemptId(appAttemptId);
    switch (job.getState()) {
      case SUCCESS:
        finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        break;
      case KILLED:
        finishReq.setFinishApplicationStatus(FinalApplicationStatus.KILLED);
        break;
      case FAILED:
        finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
        break;
      default:
        finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
    }
    this.amrmRPC.finishApplicationMaster(finishReq);
  }

  public static void main(String[] args) throws YarnRemoteException {
    // TODO we expect getting the qualified path of the job.xml as the first
    // element in the arguments
    BSPApplicationMaster master = null;
    try {
      master = new BSPApplicationMaster(args);
      master.start();
    } catch (Exception e) {
      LOG.fatal("Error starting BSPApplicationMaster", e);
    } finally {
      if (master != null) {
        master.cleanup();
      }
    }
  }

  /*
   * Some utility methods
   */

  /**
   * Reads the configuration from the given path.
   */
  private Configuration getSubmitConfiguration(String path) {
    Path jobSubmitPath = new Path(path);
    Configuration jobConf = new HamaConfiguration();
    jobConf.addResource(jobSubmitPath);
    return jobConf;
  }

  /**
   * Writes the current configuration to a given path to reflect changes. For
   * example the sync server address is put after the file has been written.
   * TODO this should upload to HDFS to a given path as well.
   * 
   * @throws IOException
   */
  private void rewriteSubmitConfiguration(String path, Configuration conf)
      throws IOException {
    Path jobSubmitPath = new Path(path);
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(jobSubmitPath);
    conf.writeXml(out);
    out.close();
    LOG.info("Written new configuration back to " + path);
  }

  /**
   * Uses Minas AvailablePortFinder to find a port, starting at 14000.
   * 
   * @return a free port.
   */
  private int getFreePort() {
    int startPort = 14000;
    return getFreePort(startPort);
  }

  /**
   * Uses Minas AvailablePortFinder to find a port, starting at startPort.
   * 
   * @return a free port.
   */
  private int getFreePort(int startPort) {
    while (!AvailablePortFinder.available(startPort)) {
      startPort++;
      LOG.debug("Testing port for availability: " + startPort);
    }
    return startPort;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return BSPClient.VERSION;
  }

  @Override
  public LongWritable getCurrentSuperStep() {
    return syncServer.getSuperStep();
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    // TODO Auto-generated method stub
    return new ProtocolSignature();
  }

}
