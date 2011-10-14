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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.HamaConfiguration;

public class YARNBSPJob extends BSPJob {

  private static final Log LOG = LogFactory.getLog(YARNBSPJob.class);

  private BSPClient client;
  private YarnRPC rpc;
  private ApplicationId id;
  private FileSystem fs;

  private boolean submitted;

  private ApplicationReport report;

  private ClientRMProtocol applicationsManager;

  public YARNBSPJob(HamaConfiguration conf) throws IOException {
    super(conf);
    rpc = YarnRPC.create(conf);
    fs = FileSystem.get(conf);
  }

  public void setMemoryUsedPerTaskInMb(int mem) {
    conf.setInt("bsp.child.mem.in.mb", mem);
  }

  public void kill() throws YarnRemoteException {
    KillApplicationRequest killRequest = Records
        .newRecord(KillApplicationRequest.class);
    killRequest.setApplicationId(id);
    applicationsManager.forceKillApplication(killRequest);
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    LOG.info("Submitting job...");
    if (conf.get("bsp.child.mem.in.mb") == null) {
      LOG.warn("BSP Child memory has not been set, YARN will guess your needs or use default values.");
    }

    if (rpc == null) {
      rpc = YarnRPC.create(getConf());
    }

    if (fs == null) {
      fs = FileSystem.get(getConf());
    }

    if (conf.get("bsp.user.name") == null) {
      String s = getUnixUserName();
      conf.set("bsp.user.name", s);
      LOG.info("Retrieved username: " + s);
    }

    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
        YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS));
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    Configuration appsManagerServerConf = new Configuration(conf);
    // TODO what is that?
    // appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_INFO,
    // ClientRMSecurityInfo.class, SecurityInfo.class);

    applicationsManager = ((ClientRMProtocol) rpc.getProxy(
        ClientRMProtocol.class, rmAddress, appsManagerServerConf));

    GetNewApplicationRequest request = Records
        .newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = applicationsManager
        .getNewApplication(request);
    id = response.getApplicationId();
    LOG.info("Got new ApplicationId=" + id);

    // Create a new ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);
    // set the ApplicationId
    appContext.setApplicationId(this.id);
    // set the application name
    appContext.setApplicationName(this.getJobName());

    // Create a new container launch context for the AM's container
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);

    // Define the local resources required
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    // Lets assume the jar we need for our ApplicationMaster is available in
    // HDFS at a certain known path to us and we want to make it available to
    // the ApplicationMaster in the launched container
    Path jarPath = new Path(getWorkingDirectory(), id + "/app.jar");
    fs.copyFromLocalFile(this.getLocalPath(this.getJar()), jarPath);
    LOG.info("Copying app jar to " + jarPath);
    conf.set("bsp.jar",
        jarPath.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString());
    FileStatus jarStatus = fs.getFileStatus(jarPath);
    LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
    amJarRsrc.setType(LocalResourceType.FILE);
    amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    amJarRsrc.setTimestamp(jarStatus.getModificationTime());
    amJarRsrc.setSize(jarStatus.getLen());
    // this creates a symlink in the working directory
    localResources.put("AppMaster.jar", amJarRsrc);
    // Set the local resources into the launch context
    amContainer.setLocalResources(localResources);

    // Set up the environment needed for the launch context
    Map<String, String> env = new HashMap<String, String>();
    // Assuming our classes or jars are available as local resources in the
    // working directory from which the command will be run, we need to append
    // "." to the path.
    // By default, all the hadoop specific classpaths will already be available
    // in $CLASSPATH, so we should be careful not to overwrite it.
    String classPathEnv = "$CLASSPATH:./*:";
    env.put("CLASSPATH", classPathEnv);
    amContainer.setEnvironment(env);

    // saving the conf file at this point to hdfs
    // it should be in HDFS.
    Path xmlPath = new Path(getWorkingDirectory(), id + "/job.xml");
    FSDataOutputStream out = fs.create(xmlPath);
    this.writeXml(out);
    out.flush();
    out.close();

    // Construct the command to be executed on the launched container
    String command = "${JAVA_HOME}"
        + "/bin/java -cp "
        + classPathEnv
        + " "
        + BSPApplicationMaster.class.getCanonicalName()
        + " "
        + xmlPath.makeQualified(fs.getUri(), fs.getWorkingDirectory())
            .toString() + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/stderr";

    LOG.info("Start command: " + command);

    amContainer.setCommands(Collections.singletonList(command));

    Resource capability = Records.newRecord(Resource.class);
    // we have at least 3 threads, which comsumes 1mb each, for each bsptask and
    // a base usage of 100mb
    capability.setMemory(3 * this.getNumBspTask() + 100);
    LOG.info("Set memory for the application master to "
        + capability.getMemory() + "mb!");
    amContainer.setResource(capability);

    // Set the container launch content into the ApplicationSubmissionContext
    appContext.setAMContainerSpec(amContainer);

    // Create the request to send to the ApplicationsManager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    applicationsManager.submitApplication(appRequest);

    GetApplicationReportRequest reportRequest = Records
        .newRecord(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(id);
    while (report == null || report.getHost().equals("N/A")) {
      GetApplicationReportResponse reportResponse = applicationsManager
          .getApplicationReport(reportRequest);
      report = reportResponse.getApplicationReport();
      Thread.sleep(1000L);
    }
    LOG.info("Got report: " + report.getApplicationId() + " "
        + report.getHost());
    submitted = true;
  }

  @Override
  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {

    LOG.info("Starting job...");

    if (!submitted) {
      this.submit();
    }

    client = (BSPClient) RPC.waitForProxy(BSPClient.class, BSPClient.VERSION,
        NetUtils.createSocketAddr(report.getHost(), report.getRpcPort()), conf);

    GetApplicationReportRequest reportRequest = Records
        .newRecord(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(id);

    GetApplicationReportResponse reportResponse = applicationsManager
        .getApplicationReport(reportRequest);
    ApplicationReport localReport = reportResponse.getApplicationReport();
    long clientSuperStep = 0L;
    // TODO this may cause infinite loops, we can go with our rpc client
    while (localReport.getFinalApplicationStatus() != null
        && localReport.getFinalApplicationStatus() != FinalApplicationStatus.FAILED
        && localReport.getFinalApplicationStatus() != FinalApplicationStatus.KILLED
        && localReport.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
      LOG.info("currently in state: " + localReport.getFinalApplicationStatus());
      if (verbose) {
        long remoteSuperStep = client.getCurrentSuperStep().get();
        if (clientSuperStep > remoteSuperStep) {
          clientSuperStep = remoteSuperStep;
          LOG.info("Current supersteps number: " + clientSuperStep);
        }
        reportResponse = applicationsManager
            .getApplicationReport(reportRequest);
        localReport = reportResponse.getApplicationReport();
      }
      Thread.sleep(3000L);
    }

    reportResponse = applicationsManager.getApplicationReport(reportRequest);
    localReport = reportResponse.getApplicationReport();

    if (localReport.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED) {
      LOG.info("Job succeeded!");
      return true;
    } else {
      LOG.info("Job failed with status: "
          + localReport.getFinalApplicationStatus().toString() + "!");
      return false;
    }

  }

  /*
   * THESE FOLLOWING METHODS WILL BE IMPLEMENTED IN BSPJOBCLIENT SOON.
   */

  static String getUnixUserName() throws IOException {
    String[] result = executeShellCommand(new String[] { Shell.USER_NAME_COMMAND });
    if (result.length != 1) {
      throw new IOException("Expect one token as the result of "
          + Shell.USER_NAME_COMMAND + ": " + toString(result));
    }
    return result[0];
  }

  private static String toString(String[] strArray) {
    if (strArray == null || strArray.length == 0) {
      return "";
    }
    StringBuilder buf = new StringBuilder(strArray[0]);
    for (int i = 1; i < strArray.length; i++) {
      buf.append(' ');
      buf.append(strArray[i]);
    }
    return buf.toString();
  }

  private static String[] executeShellCommand(String[] command)
      throws IOException {
    String groups = Shell.execCommand(command);
    StringTokenizer tokenizer = new StringTokenizer(groups);
    int numOfTokens = tokenizer.countTokens();
    String[] tokens = new String[numOfTokens];
    for (int i = 0; tokenizer.hasMoreTokens(); i++) {
      tokens[i] = tokenizer.nextToken();
    }

    return tokens;
  }

}
