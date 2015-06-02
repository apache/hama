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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.HamaConfiguration;

public class YARNBSPJobClient extends BSPJobClient {

  private static final Log LOG = LogFactory.getLog(YARNBSPJobClient.class);

  private ApplicationId id;
  private ApplicationReport report;

  // Configuration
  private YarnClient yarnClient;
  private YarnConfiguration yarnConf;

  // Start time for client
  private final long clientStartTime = System.currentTimeMillis();
  // Timeout threshold for client. Kill app after time interval expires.
  private long clientTimeout = 60000;

  class NetworkedJob implements RunningJob {
    @Override
    public BSPJobID getID() {
      return null;
    }

    @Override
    public String getJobName() {
      return null;
    }

    @Override
    public long progress() throws IOException {
      return 0;
    }

    @Override
    public boolean isComplete() throws IOException {
      return false;
    }

    @Override
    public boolean isSuccessful() throws IOException {
      return false;
    }

    @Override
    public void waitForCompletion() throws IOException {

    }

    @Override
    public int getJobState() throws IOException {
      return 0;
    }

    @Override
    public void killJob() throws IOException {

    }

    @Override
    public void killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {

    }

    @Override
    public long getSuperstepCount() throws IOException {
      return 0;
    }

    @Override
    public JobStatus getStatus() {
      return null;
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(int eventCounter) {
      return new TaskCompletionEvent[0];
    }

    @Override
    public String getJobFile() {
      return null;
    }
  }

  public YARNBSPJobClient(HamaConfiguration conf) {
    setConf(conf);
    yarnConf = new YarnConfiguration(conf);
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(yarnConf);
  }

  @Override
  protected RunningJob launchJob(BSPJobID jobId, BSPJob normalJob,
      Path submitJobFile, FileSystem pFs) throws IOException {
    YARNBSPJob job = (YARNBSPJob) normalJob;

    LOG.info("Submitting job...");
    if (getConf().get("bsp.child.mem.in.mb") == null) {
      LOG.warn("BSP Child memory has not been set, YARN will guess your needs or use default values.");
    }

    FileSystem fs = pFs;
    if (fs == null) {
      fs = FileSystem.get(getConf());
    }

    if (getConf().get("bsp.user.name") == null) {
      String s = getUnixUserName();
      getConf().set("bsp.user.name", s);
      LOG.debug("Retrieved username: " + s);
    }

    yarnClient.start();
    try {
      YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
      LOG.info("Got Cluster metric info from ASM"
          + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

      List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
          NodeState.RUNNING);
      LOG.info("Got Cluster node info from ASM");
      for (NodeReport node : clusterNodeReports) {
        LOG.info("Got node report from ASM for"
            + ", nodeId=" + node.getNodeId()
            + ", nodeAddress" + node.getHttpAddress()
            + ", nodeRackName" + node.getRackName()
            + ", nodeNumContainers" + node.getNumContainers());
      }

      QueueInfo queueInfo = yarnClient.getQueueInfo("default");
      LOG.info("Queue info"
          + ", queueName=" + queueInfo.getQueueName()
          + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
          + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
          + ", queueApplicationCount=" + queueInfo.getApplications().size()
          + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

      List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
      for (QueueUserACLInfo aclInfo : listAclInfo) {
        for (QueueACL userAcl : aclInfo.getUserAcls()) {
          LOG.info("User ACL Info for Queue"
              + ", queueName=" + aclInfo.getQueueName()
              + ", userAcl=" + userAcl.name());
        }
      }

      // Get a new application id
      YarnClientApplication app = yarnClient.createApplication();


      // Create a new ApplicationSubmissionContext
      //ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
      ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

      id = appContext.getApplicationId();

      // set the application name
      appContext.setApplicationName(job.getJobName());

      // Create a new container launch context for the AM's container
      ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

      // Define the local resources required
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
      // Lets assume the jar we need for our ApplicationMaster is available in
      // HDFS at a certain known path to us and we want to make it available to
      // the ApplicationMaster in the launched container
      if (job.getJar() == null) {
        throw new IllegalArgumentException("Jar must be set in order to run the application!");
      }
      
      Path jarPath = new Path(job.getJar());
      jarPath = fs.makeQualified(jarPath);
      getConf().set("bsp.jar", jarPath.makeQualified(fs.getUri(), jarPath).toString());

      FileStatus jarStatus = fs.getFileStatus(jarPath);
      LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
      amJarRsrc.setType(LocalResourceType.FILE);
      amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
      amJarRsrc.setTimestamp(jarStatus.getModificationTime());
      amJarRsrc.setSize(jarStatus.getLen());

      // this creates a symlink in the working directory
      localResources.put(YARNBSPConstants.APP_MASTER_JAR_PATH, amJarRsrc);

      // add hama related jar files to localresources for container
      List<File> hamaJars;
      if (System.getProperty("hama.home.dir") != null)
        hamaJars = localJarfromPath(System.getProperty("hama.home.dir"));
      else
        hamaJars = localJarfromPath(getConf().get("hama.home.dir"));
      String hamaPath = getSystemDir() + "/hama";
      for (File fileEntry : hamaJars) {
        addToLocalResources(fs, fileEntry.getCanonicalPath(),
            hamaPath, fileEntry.getName(), localResources);
      }

      // Set the local resources into the launch context
      amContainer.setLocalResources(localResources);

      // Set up the environment needed for the launch context
      Map<String, String> env = new HashMap<String, String>();
      // Assuming our classes or jars are available as local resources in the
      // working directory from which the command will be run, we need to append
      // "." to the path.
      // By default, all the hadoop specific classpaths will already be available
      // in $CLASSPATH, so we should be careful not to overwrite it.
      StringBuilder classPathEnv = new StringBuilder(
          ApplicationConstants.Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
          .append("./*");
      for (String c : yarnConf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(c.trim());
      }

      env.put(YARNBSPConstants.HAMA_YARN_LOCATION, jarPath.toUri().toString());
      env.put(YARNBSPConstants.HAMA_YARN_SIZE, Long.toString(jarStatus.getLen()));
      env.put(YARNBSPConstants.HAMA_YARN_TIMESTAMP, Long.toString(jarStatus.getModificationTime()));

      env.put(YARNBSPConstants.HAMA_LOCATION, hamaPath);
      env.put("CLASSPATH", classPathEnv.toString());
      amContainer.setEnvironment(env);

      // Set the necessary command to execute on the allocated container
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);
      vargs.add("${JAVA_HOME}/bin/java");
      vargs.add("-cp " + classPathEnv + "");
      vargs.add(ApplicationMaster.class.getCanonicalName());
      vargs.add(submitJobFile.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString());

      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/hama-appmaster.stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/hama-appmaster.stderr");

      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());
      amContainer.setCommands(commands);

      LOG.debug("Start command: " + command);

      Resource capability = Records.newRecord(Resource.class);
      // we have at least 3 threads, which comsumes 1mb each, for each bsptask and
      // a base usage of 100mb
      capability.setMemory(3 * job.getNumBspTask() + getConf().getInt("hama.appmaster.memory.mb", 100));
      LOG.info("Set memory for the application master to " + capability.getMemory() + "mb!");

      // Set the container launch content into the ApplicationSubmissionContext
      appContext.setResource(capability);

      // Setup security tokens
      if (UserGroupInformation.isSecurityEnabled()) {
        // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
        Credentials credentials = new Credentials();
        String tokenRenewer = yarnConf.get(YarnConfiguration.RM_PRINCIPAL);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
          throw new IOException(
              "Can't get Master Kerberos principal for the RM to use as renewer");
        }

        // For now, only getting tokens for the default file-system.
        final Token<?> tokens[] =
            fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
          for (Token<?> token : tokens) {
            LOG.info("Got dt for " + fs.getUri() + "; " + token);
          }
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        amContainer.setTokens(fsTokens);
      }

      appContext.setAMContainerSpec(amContainer);

      // Create the request to send to the ApplicationsManager
      ApplicationId appId = appContext.getApplicationId();
      yarnClient.submitApplication(appContext);

      return monitorApplication(appId) ? new NetworkedJob() : null;
    } catch (YarnException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public Path getSystemDir() {
    return new Path(getConf().get("bsp.local.dir", "/tmp/hama-yarn/"));
  }

  ApplicationId getId() {
    return id;
  }

  public ApplicationReport getReport() {
    return report;
  }

  private boolean monitorApplication(ApplicationId appId)
      throws IOException, YarnException {
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      report = yarnClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", clientToAMToken="
          + report.getClientToAMToken() + ", appDiagnostics="
          + report.getDiagnostics() + ", appMasterHost="
          + report.getHost() + ", appQueue=" + report.getQueue()
          + ", appMasterRpcPort=" + report.getRpcPort()
          + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState="
          + report.getYarnApplicationState().toString()
          + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl()
          + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report
          .getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully."
              + " YarnState=" + state.toString()
              + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
          || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish." + " YarnState="
            + state.toString() + ", DSFinalStatus="
            + dsStatus.toString() + ". Breaking monitoring loop");
        return false;
      }

      if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
        LOG.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(appId);
        return false;
      }
    }
  }

  /**
   * Kill a submitted application by sending a call to the ASM
   * @param appId Application Id to be killed.
   * @throws YarnException
   * @throws IOException
   */
  private void forceKillApplication(ApplicationId appId)
      throws YarnException, IOException {
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
    // the same time.
    // If yes, can we kill a particular attempt only?

    // Response can be ignored as it is non-null on success or
    // throws an exception in case of failures
    yarnClient.killApplication(appId);
  }

  private List<File> localJarfromPath(String path) throws IOException {
    File hamaHome = new File(path);
    String[] extensions = new String[]{"jar"};
    List<File> files = (List<File>)FileUtils.listFiles(hamaHome, extensions, true);

    return files;
  }

  private void addToLocalResources(FileSystem fs, String fileSrcPath,
      String fileDstPath, String fileName, Map<String, LocalResource> localResources)
      throws IOException {
    Path dstPath = new Path(fileDstPath, fileName);
    dstPath = fs.makeQualified(dstPath);
    fs.copyFromLocalFile(false, true, new Path(fileSrcPath), dstPath);
    FileStatus fileStatus = fs.getFileStatus(dstPath);
    LocalResource localRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dstPath.toUri()),
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
            fileStatus.getLen(), fileStatus.getModificationTime());
    localResources.put(fileName, localRsrc);
  }
}
