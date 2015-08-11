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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.SyncServer;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.ipc.RPC;
import org.apache.hama.ipc.Server;
import org.apache.hama.util.BSPNetUtils;
import org.apache.log4j.LogManager;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationMaster implements BSPClient, BSPPeerProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  // Configuration
  private Configuration localConf;
  private Configuration jobConf;

  private String jobFile;
  private String applicationName;
  // RPC info where the AM receive client side requests
  private String hostname;
  private int clientPort;
  private FileSystem fs;

  private volatile long superstep;
  private Counters globalCounter = new Counters();
  private BSPJobClient.RawSplit[] splits;

  // Hama job id
  private BSPJobID jobId;
  // Partiion id
  private static AtomicInteger ai = new AtomicInteger(-1);

  // SyncServer for Zookeeper
  private SyncServer syncServer;

  // Zookeeper thread pool
  private static final ExecutorService threadPool = Executors
      .newFixedThreadPool(1);

  // RPC info where the AM receive client side requests
  private int taskServerPort;

  private Server clientServer;
  private Server taskServer;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;

  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;

  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;

  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  protected ApplicationAttemptId appAttemptID;

  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  // App Master configuration
  // No. of containers to run shell command on
  @VisibleForTesting
  protected int numTotalContainers;
  // Memory to request for the container on which the shell command will run
  private int containerMemory;
  // VirtualCores to request for the container on which the shell command will
  // run
  private int containerVirtualCores = 1;

  // Priority of the request
  private int requestPriority = 0;

  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  protected AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  protected AtomicInteger numRequestedContainers = new AtomicInteger();

  private volatile boolean done;
  private ByteBuffer allTokens;

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers = Collections
      .newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  public ApplicationMaster() {
    // Set up the configuration
    this.localConf = new YarnConfiguration();
  }

  public static void main(String[] args) throws IOException {
    boolean result = false;
    ApplicationMaster appMaster = new ApplicationMaster();

    try {
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
    } finally {
      LOG.info("Stop SyncServer and RPCServer.");
      appMaster.close();
    }

    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  public boolean init(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException();
    }
    this.jobFile = args[0];
    this.jobConf = getSubmitConfiguration(jobFile);
    localConf.addResource(localConf);
    fs = FileSystem.get(jobConf);

    this.applicationName = jobConf.get("bsp.job.name",
        "<no bsp job name defined>");
    if (applicationName.isEmpty()) {
      this.applicationName = "<no bsp job name defined>";
    }

    appAttemptID = getApplicationAttemptId();
    this.jobId = new BSPJobID(appAttemptID.toString(), 0);
    this.appMasterHostname = BSPNetUtils.getCanonicalHostname();
    this.appMasterTrackingUrl = "http://localhost:8088";
    this.numTotalContainers = this.jobConf.getInt("bsp.peers.num", 1);
    this.containerMemory = getMemoryRequirements(jobConf);

    this.hostname = BSPNetUtils.getCanonicalHostname();
    this.clientPort = BSPNetUtils.getFreePort(12000);

    // Set configuration for starting SyncServer which run Zookeeper
    this.jobConf.set(Constants.ZOOKEEPER_QUORUM, appMasterHostname);

    // start our synchronization service
    startSyncServer();

    // start RPC server
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

    return true;
  }

  /**
   * Main run function for the application master
   * 
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws IOException
   */
  @SuppressWarnings({ "unchecked" })
  public void run() throws YarnException, IOException, InterruptedException {
    LOG.info("Starting ApplicationMaster");

    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials = UserGroupInformation.getCurrentUser()
        .getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName = System
        .getenv(ApplicationConstants.Environment.USER.name());
    appSubmitterUgi = UserGroupInformation
        .createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);

    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(localConf);
    amRMClient.start();

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(localConf);
    nmClientAsync.start();

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
            appMasterTrackingUrl);
    // Dump out information about cluster capability as seen by the
    // resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    LOG.info("Max mem capability of resources in this cluster " + maxMem);

    int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
    LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

    // A resource ask cannot exceed the max.
    if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerMemory + ", max="
          + maxMem);
      containerMemory = maxMem;
    }

    if (containerVirtualCores > maxVCores) {
      LOG.info("Container virtual cores specified above max threshold of cluster."
          + " Using max value."
          + ", specified="
          + containerVirtualCores
          + ", max=" + maxVCores);
      containerVirtualCores = maxVCores;
    }

    List<Container> previousAMRunningContainers = response
        .getContainersFromPreviousAttempts();
    LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
        + " previous attempts' running containers on AM registration.");
    for (Container container : previousAMRunningContainers) {
      launchedContainers.add(container.getId());
    }
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

    int numTotalContainersToRequest = numTotalContainers
        - previousAMRunningContainers.size();
    // Setup ask for containers from RM
    // Send request for containers to RM
    // Until we get our fully allocated quota, we keep on polling RM for
    // containers
    // Keep looping until all the containers are launched and shell script
    // executed on them ( regardless of success/failure).
    for (int i = 0; i < numTotalContainersToRequest; ++i) {
      AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
      amRMClient.addContainerRequest(containerAsk);
    }
    numRequestedContainers.set(numTotalContainers);
  }

  @VisibleForTesting
  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler(this);
  }

  @VisibleForTesting
  protected boolean finish() {
    // wait for completion.
    while (!done && (numCompletedContainers.get() != numTotalContainers)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
      }
    }

    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      } catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: " + e.getMessage());
        e.printStackTrace();
      }
    }

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (numFailedContainers.get() == 0
        && numCompletedContainers.get() == numTotalContainers) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers
          + ", completed=" + numCompletedContainers.get() + ", allocated="
          + numAllocatedContainers.get() + ", failed="
          + numFailedContainers.get();
      LOG.info(appMessage);
      success = false;
    }
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }

    amRMClient.stop();

    return success;
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
          + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info(appAttemptID + " got container status for containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        // ignore containers we know nothing about - probably from a previous
        // attempt
        if (!launchedContainers.contains(containerStatus.getContainerId())) {
          LOG.info("Ignoring completed status of "
              + containerStatus.getContainerId()
              + "; unknown container(probably launched by previous attempt)");
          continue;
        }

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // shell script failed
            // counts as completed
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            // we do not need to release the container as it would be done
            // by the RM
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
      }

      // ask for more containers if any failed
      int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      if (askCount > 0) {
        for (int i = 0; i < askCount; ++i) {
          AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
          amRMClient.addContainerRequest(containerAsk);
        }
      }

      if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt="
          + allocatedContainers.size());

      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        LOG.info("Launching shell command on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerResourceMemory"
            + allocatedContainer.getResource().getMemory()
            + ", containerResourceVirtualCores"
            + allocatedContainer.getResource().getVirtualCores());

        Thread launchThread = createLaunchContainerThread(allocatedContainer);

        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchedContainers.add(allocatedContainer.getId());
        launchThread.start();
      }
    }

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) numCompletedContainers.get()
          / numTotalContainers;
      return progress;
    }

    @Override
    public void onError(Throwable throwable) {
      done = true;
      amRMClient.stop();
    }
  }

  @VisibleForTesting
  static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

    private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;

    public NMCallbackHandler(ApplicationMaster applicationMaster) {
      this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status="
            + containerStatus);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(containerId,
            container.getNodeId());
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
      applicationMaster.numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }
  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the
   * container that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    Container container;

    NMCallbackHandler containerListener;

    Configuration conf;

    /**
     * @param lcontainer Allocated container
     * @param containerListener Callback handler of the container
     */
    public LaunchContainerRunnable(Container lcontainer,
        NMCallbackHandler containerListener, Configuration conf) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.conf = conf;
    }

    /**
     * Connects to CM, sets up container launch context for shell command and
     * eventually dispatches the container start request to the CM.
     */
    @Override
    public void run() {
      LOG.info("Setting up container launch container for containerid="
          + container.getId());
      // Now we setup a ContainerLaunchContext
      ContainerLaunchContext ctx = Records
          .newRecord(ContainerLaunchContext.class);

      // Set the local resources
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
      LocalResource packageResource = Records.newRecord(LocalResource.class);
      FileSystem fs = null;
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        e.printStackTrace();
      }
      Path packageFile = new Path(
          System.getenv(YARNBSPConstants.HAMA_YARN_LOCATION));
      URL packageUrl = null;
      try {
        packageUrl = ConverterUtils.getYarnUrlFromPath(packageFile
            .makeQualified(fs.getUri(), fs.getWorkingDirectory()));
        LOG.info("PackageURL has been composed to " + packageUrl.toString());
        LOG.info("Reverting packageURL to path: "
            + ConverterUtils.getPathFromYarnURL(packageUrl));
      } catch (URISyntaxException e) {
        LOG.fatal("If you see this error the workarround does not work", e);
        numCompletedContainers.incrementAndGet();
        numFailedContainers.incrementAndGet();
        return;
      }

      packageResource.setResource(packageUrl);
      packageResource.setSize(Long.parseLong(System
          .getenv(YARNBSPConstants.HAMA_YARN_SIZE)));
      packageResource.setTimestamp(Long.parseLong(System
          .getenv(YARNBSPConstants.HAMA_YARN_TIMESTAMP)));
      packageResource.setType(LocalResourceType.FILE);
      packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

      localResources.put(YARNBSPConstants.APP_MASTER_JAR_PATH, packageResource);

      Path hamaReleaseFile = new Path(
          System.getenv(YARNBSPConstants.HAMA_LOCATION));
      URL hamaReleaseUrl = ConverterUtils.getYarnUrlFromPath(hamaReleaseFile
          .makeQualified(fs.getUri(), fs.getWorkingDirectory()));
      LOG.info("Hama release URL has been composed to "
          + hamaReleaseUrl.toString());

      RemoteIterator<LocatedFileStatus> fileStatusListIterator = null;
      try {
        fileStatusListIterator = fs.listFiles(hamaReleaseFile, true);

        while (fileStatusListIterator.hasNext()) {
          LocatedFileStatus lfs = fileStatusListIterator.next();
          LocalResource localRsrc = LocalResource.newInstance(
              ConverterUtils.getYarnUrlFromPath(lfs.getPath()),
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
              lfs.getLen(), lfs.getModificationTime());
          localResources.put(lfs.getPath().getName(), localRsrc);
        }
      } catch (IOException e) {
        LOG.fatal("The error has occured to RemoteIterator  " + e);
      }

      ctx.setLocalResources(localResources);

      /*
       * TODO Package classpath seems not to work if you're in pseudo
       * distributed mode, because the resource must not be moved, it will never
       * be unpacked. So we will check if our jar file has the file:// prefix
       * and put it into the CP directly
       */

      StringBuilder classPathEnv = new StringBuilder(
          ApplicationConstants.Environment.CLASSPATH.$()).append(
          File.pathSeparatorChar).append("./*");
      for (String c : conf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(c.trim());
      }

      Vector<CharSequence> vargs = new Vector<CharSequence>();
      vargs.add("${JAVA_HOME}/bin/java");
      vargs.add("-cp " + classPathEnv + "");
      vargs.add(BSPRunner.class.getCanonicalName());

      vargs.add(jobId.getJtIdentifier());
      vargs.add(Integer.toString(ai.incrementAndGet()));
      vargs.add(new Path(jobFile).makeQualified(fs.getUri(),
          fs.getWorkingDirectory()).toString());

      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
          + "/hama-worker.stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
          + "/hama-worker.stderr");

      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());

      ctx.setCommands(commands);
      ctx.setTokens(allTokens.duplicate());
      LOG.info("Starting commands: " + commands);

      containerListener.addContainer(container.getId(), container);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * 
   * @return the setup ResourceRequest to be sent to RM
   */
  private AMRMClient.ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(requestPriority);

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu
    // requirements
    Resource capability = Resource.newInstance(containerMemory,
        containerVirtualCores);

    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(
        capability, null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  /**
   * Reads the configuration from the given path.
   */
  private static Configuration getSubmitConfiguration(String path)
      throws IOException {
    Path jobSubmitPath = new Path(path);
    Configuration jobConf = new HamaConfiguration();

    FileSystem fs = FileSystem.get(URI.create(path), jobConf);

    InputStream in = fs.open(jobSubmitPath);
    jobConf.addResource(in);

    return jobConf;
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
    if (!envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID.name())) {
      throw new IllegalArgumentException(
          "ApplicationAttemptId not set in the environment");
    }

    ContainerId containerId = ConverterUtils.toContainerId(envs
        .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    return containerId.getApplicationAttemptId();
  }

  /**
   * This method starts the sync server on a specific port and waits for it to
   * come up. Be aware that this method adds the "bsp.sync.server.address" that
   * is needed for a task to connect to the service.
   * 
   * @throws IOException
   */
  private void startSyncServer() throws Exception {
    syncServer = SyncServiceFactory.getSyncServer(jobConf);
    syncServer.init(jobConf);

    ZKServerThread serverThread = new ZKServerThread(syncServer);
    threadPool.submit(serverThread);
  }

  /**
   * This method is to run Zookeeper in order to coordinates between BSPMaster
   * and Groomservers using Runnable interface in java.
   */
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
        LOG.error("Error running SyncServer.", e);
      }
    }
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
    this.clientServer = RPC.getServer(BSPClient.class, hostname, clientPort,
        jobConf);
    this.clientServer.start();

    // start the RPC server which talks to the tasks
    this.taskServerPort = BSPNetUtils.getFreePort(10000);
    this.taskServer = RPC.getServer(this, hostname, taskServerPort, jobConf);
    this.taskServer.start();

    // readjusting the configuration to let the tasks know where we are.
    this.jobConf.set("hama.umbilical.address", hostname + ":" + taskServerPort);
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

  /**
   * Get container memory from "bsp.child.mem.in.mb" set on Hama configuration
   * 
   * @return The memory of container.
   */
  private int getMemoryRequirements(Configuration conf) {
    String newMemoryProperty = conf.get("bsp.child.mem.in.mb");
    if (newMemoryProperty == null) {
      LOG.warn("\"bsp.child.mem.in.mb\" was not set! Try parsing the child opts...");
      return getMemoryFromOptString(conf.get("bsp.child.java.opts"));
    } else {
      return Integer.valueOf(newMemoryProperty);
    }
  }

  // This really needs a testcase
  private static int getMemoryFromOptString(String opts) {
    final int DEFAULT_MEMORY_MB = 256;

    if (opts == null) {
      return DEFAULT_MEMORY_MB;
    }

    if (!opts.contains("-Xmx")) {
      LOG.info("No \"-Xmx\" option found in child opts, using default amount of memory!");
      return DEFAULT_MEMORY_MB;
    } else {
      // e.G: -Xmx512m

      int startIndex = opts.indexOf("-Xmx") + 4;
      String xmxString = opts.substring(startIndex);
      char qualifier = xmxString.charAt(xmxString.length() - 1);
      int memory = Integer.valueOf(xmxString.substring(0,
          xmxString.length() - 1));
      if (qualifier == 'm') {
        return memory;
      } else if (qualifier == 'g') {
        return memory * 1024;
      } else {
        throw new IllegalArgumentException(
            "Memory Limit in child opts was not set! \"bsp.child.java.opts\" String was: "
                + opts);
      }
    }
  }

  @VisibleForTesting
  Thread createLaunchContainerThread(Container allocatedContainer) {
    LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
        allocatedContainer, containerListener, jobConf);
    return new Thread(runnableLaunchContainer);
  }

  @Override
  public LongWritable getCurrentSuperStep() {
    return new LongWritable(superstep);
  }

  @Override
  public Task getTask(TaskAttemptID taskid) throws IOException {
    BSPJobClient.RawSplit assignedSplit = null;
    String splitName = NullInputFormat.NullInputSplit.class.getName();
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

  @Override
  public int getAssignedPortNum(TaskAttemptID taskid) {
    return 0;
  }

  @Override
  public void close() throws IOException {
    this.clientServer.stop();
    this.taskServer.stop();
    this.syncServer.stopServer();
    threadPool.shutdown();
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return BSPClient.versionID;
  }
}
