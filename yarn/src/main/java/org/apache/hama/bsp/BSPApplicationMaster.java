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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Job.JobState;
import org.apache.hama.bsp.sync.SyncServerRunner;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.ipc.RPC;
import org.apache.hama.ipc.Server;
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

  private ApplicationMasterProtocol amrmRPC;

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

  private FileSystem fs;
  private BSPJobClient.RawSplit[] splits;

  private BSPApplicationMaster(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException();
    }

    this.jobFile = args[0];
    this.localConf = new YarnConfiguration();
    this.jobConf = getSubmitConfiguration(jobFile);
    fs = FileSystem.get(jobConf);

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

    String jobSplit = jobConf.get("bsp.job.split.file");
    splits = null;
    if (jobSplit != null) {
      DataInputStream splitFile = fs.open(new Path(jobSplit));
      try {
        splits = BSPJobClient.readSplitFile(splitFile);
      } finally {
        splitFile.close();
      }
    }

    this.amrmRPC = getYarnRPCConnection(localConf);
    registerApplicationMaster(amrmRPC, hostname, clientPort,
        "http://localhost:8080");
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
    this.clientServer = RPC.getServer(BSPClient.class, hostname, clientPort, jobConf);
    this.clientServer.start();

    // start the RPC server which talks to the tasks
    this.taskServerPort = BSPNetUtils.getFreePort(10000);
    this.taskServer = RPC.getServer(this, hostname, taskServerPort, jobConf);
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
  private ApplicationMasterProtocol getYarnRPCConnection(Configuration yarnConf) throws IOException {
    // Connect to the Scheduler of the ResourceManager.
    UserGroupInformation currentUser = UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();

    final InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));

    Token<? extends TokenIdentifier> amRMToken = setupAndReturnAMRMToken(rmAddress, credentials.getAllTokens());
    currentUser.addToken(amRMToken);

    final Configuration conf = yarnConf;

    ApplicationMasterProtocol client = currentUser
        .doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) yarnRPC.getProxy(ApplicationMasterProtocol.class, rmAddress, conf);
          }
        });
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return client;
  }

  private Token<? extends TokenIdentifier> setupAndReturnAMRMToken(
      InetSocketAddress rmBindAddress,
      Collection<Token<? extends TokenIdentifier>> allTokens) {
    for (Token<? extends TokenIdentifier> token : allTokens) {
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        SecurityUtil.setTokenService(token, rmBindAddress);
        return token;
      }
    }
    return null;
  }


  /**
   * Registers this application master with the Resource Manager and retrieves a
   * response which is used to launch additional containers.
   */
  private static RegisterApplicationMasterResponse registerApplicationMaster(
      ApplicationMasterProtocol resourceManager, String appMasterHostName, int appMasterRpcPort,
      String appMasterTrackingUrl) throws YarnException, IOException {

    RegisterApplicationMasterRequest appMasterRequest = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    appMasterRequest.setHost(appMasterHostName);
    appMasterRequest.setRpcPort(appMasterRpcPort);
    // TODO tracking URL
    appMasterRequest.setTrackingUrl(appMasterTrackingUrl);
    RegisterApplicationMasterResponse response = resourceManager
        .registerApplicationMaster(appMasterRequest);
    LOG.info("ApplicationMaster has maximum resource capability of: "
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
  private static ApplicationAttemptId getApplicationAttemptId()
      throws IOException {
    Map<String, String> envs = System.getenv();
    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      throw new IllegalArgumentException(
          "ApplicationAttemptId not set in the environment");
    }

    return ConverterUtils.toContainerId(
        envs.get(Environment.CONTAINER_ID.name()))
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

  private void cleanup() throws YarnException, IOException {
    syncServer.stop();

    if (threadPool != null && !threadPool.isShutdown()) {
      threadPool.shutdownNow();
    }

    clientServer.stop();
    taskServer.stop();
    FinishApplicationMasterRequest finishReq = Records
        .newRecord(FinishApplicationMasterRequest.class);
    switch (job.getState()) {
      case SUCCESS:
        finishReq.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
        break;
      case KILLED:
        finishReq.setFinalApplicationStatus(FinalApplicationStatus.KILLED);
        break;
      case FAILED:
        finishReq.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
        break;
      default:
        finishReq.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
    }
    this.amrmRPC.finishApplicationMaster(finishReq);
  }

  public static void main(String[] args) throws YarnException, IOException {
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

  /**
   * Reads the configuration from the given path.
   */
  private static Configuration getSubmitConfiguration(String path)
      throws IOException {
    Path jobSubmitPath = new Path(path);
    Configuration jobConf = new HamaConfiguration();

    FileSystem fs = FileSystem.get(URI.create(path), jobConf);

    InputStream in =fs.open(jobSubmitPath);
    jobConf.addResource(in);

    return jobConf;
  }

  /**
   * Writes the current configuration to a given path to reflect changes. For
   * example the sync server address is put after the file has been written.
   */
  private static void rewriteSubmitConfiguration(String path, Configuration conf)
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
    BSPJobClient.RawSplit assignedSplit = null;
    String splitName = NullInputFormat.NullInputSplit.class.getName();
    //String splitName = NullInputSplit.class.getCanonicalName();
    if (splits != null) {
      assignedSplit = splits[taskid.id];
      splitName = assignedSplit.getClassName();
      return new BSPTask(jobId, jobFile, taskid, taskid.id, splitName,
          assignedSplit.getBytes());
    } else {
      return new BSPTask(jobId, jobFile, taskid, taskid.id, splitName,
          new BytesWritable());
    }
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
