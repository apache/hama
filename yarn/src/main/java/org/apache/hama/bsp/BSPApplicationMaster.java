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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import org.apache.hama.bsp.sync.SyncServerRunner;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.BSPNetUtils;

/**
 * BSPApplicationMaster is an application master for Apache Hamas BSP Engine.
 */
public class BSPApplicationMaster implements BSPClient, BSPPeerProtocol {

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

  // RPC info where the AM receive client side requests
  private String hostname;
  private int clientPort;
  private int taskServerPort;

  private Server clientServer;
  private Server taskServer;

  private volatile long superstep;
  private SyncServerRunner syncServer;

  private Counters globalCounter = new Counters();

  private BSPApplicationMaster(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException();
    }

    this.jobFile = args[0];
    this.localConf = new YarnConfiguration();
    this.jobConf = getSubmitConfiguration(jobFile);

    this.applicationName = jobConf.get("bsp.job.name",
        "<no bsp job name defined>");
    if (applicationName.isEmpty()) {
      this.applicationName = "<no bsp job name defined>";
    }

    this.appAttemptId = getApplicationAttemptId();

    this.yarnRPC = YarnRPC.create(localConf);
    this.clock = new SystemClock();
    this.startTime = clock.getTime();

    this.jobId = new BSPJobID(appAttemptId.toString(), 0);

    this.hostname = BSPNetUtils.getCanonicalHostname();
    this.clientPort = BSPNetUtils.getFreePort(12000);

    // start our synchronization service
    startSyncServer();

    startRPCServers();
    /*
     * Make sure that this executes after the start the RPC servers, because we
     * are readjusting the configuration.
     */
    rewriteSubmitConfiguration(jobFile, jobConf);

    this.amrmRPC = getYarnRPCConnection(localConf);
    registerApplicationMaster(amrmRPC, appAttemptId, hostname, clientPort, null);
  }

  /**
   * This method starts the needed RPC servers: client server and the task
   * server. This method manipulates the configuration and therefore needs to be
   * executed BEFORE the submitconfiguration gets rewritten.
   * 
   * @throws IOException
   */
  private void startRPCServers() throws IOException {
    // start the RPC server which talks to the client
    this.clientServer = RPC.getServer(BSPClient.class, this, hostname,
        clientPort, jobConf);
    this.clientServer.start();

    // start the RPC server which talks to the tasks
    this.taskServerPort = BSPNetUtils.getFreePort(10000);
    this.taskServer = RPC.getServer(BSPPeerProtocol.class, this, hostname,
        taskServerPort, jobConf);
    this.taskServer.start();
    // readjusting the configuration to let the tasks know where we are.
    this.jobConf.set("hama.umbilical.address", hostname + ":" + taskServerPort);
  }

  /**
   * This method starts the sync server on a specific port and waits for it to
   * come up. Be aware that this method adds the "bsp.sync.server.address" that
   * is needed for a task to connect to the service.
   * 
   * @throws IOException
   */
  private void startSyncServer() throws Exception {
    syncServer = SyncServiceFactory.getSyncServerRunner(jobConf);
    jobConf = syncServer.init(jobConf);
    threadPool.submit(syncServer);
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
    if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
      throw new IllegalArgumentException(
          "ApplicationAttemptId not set in the environment");
    }
    return ConverterUtils.toContainerId(
        envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV))
        .getApplicationAttemptId();
  }

  private void start() throws Exception {
    JobState finalState = null;
    try {
      job = new JobImpl(appAttemptId, jobConf, yarnRPC, amrmRPC, jobFile, jobId);
      finalState = job.startJob();
    } finally {
      if (finalState != null) {
        LOG.info("Job \"" + applicationName + "\"'s state after completion: "
            + finalState.toString());
        LOG.info("Job took " + ((clock.getTime() - startTime) / 1000L)
            + "s to finish!");
      }
      job.cleanup();
    }
  }

  private void cleanup() throws YarnRemoteException {
    syncServer.stop();
    if (threadPool != null && !threadPool.isShutdown()) {
      threadPool.shutdownNow();
    }
    clientServer.stop();
    taskServer.stop();
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
    // we expect getting the qualified path of the job.xml as the first
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

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return BSPClient.versionID;
  }

  @Override
  public LongWritable getCurrentSuperStep() {
    return new LongWritable(superstep);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return new ProtocolSignature(BSPPeerProtocol.versionID, null);
  }

  @Override
  public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    if (taskStatus.getSuperstepCount() > superstep) {
      superstep = taskStatus.getSuperstepCount();
      LOG.info("Now in superstep " + superstep);
    }

    Counters counters = taskStatus.getCounters();
    globalCounter.incrAllCounters(counters);

    return true;
  }

  /**
   * most of the following methods are already handled over YARN and with the
   * JobImpl.
   */

  @Override
  public void close() throws IOException {

  }

  @Override
  public Task getTask(TaskAttemptID taskid) throws IOException {
    return null;
  }

  @Override
  public boolean ping(TaskAttemptID taskid) throws IOException {
    return false;
  }

  @Override
  public void done(TaskAttemptID taskid) throws IOException {

  }

  @Override
  public void fsError(TaskAttemptID taskId, String message) throws IOException {

  }

  @Override
  public void fatalError(TaskAttemptID taskId, String message)
      throws IOException {

  }

  @Override
  public int getAssignedPortNum(TaskAttemptID taskid) {
    return 0;
  }

}
