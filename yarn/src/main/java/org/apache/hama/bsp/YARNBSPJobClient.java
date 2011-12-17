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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.EnvironmentUtil;

public class YARNBSPJobClient extends BSPJobClient {

  private static final Log LOG = LogFactory.getLog(YARNBSPJobClient.class);

  private ApplicationId id;
  private ApplicationReport report;

  public YARNBSPJobClient(HamaConfiguration conf) {
    setConf(conf);
  }

  @Override
  protected RunningJob launchJob(BSPJobID jobId, BSPJob normalJob,
      Path submitJobFile, FileSystem fs) throws IOException {

    YARNBSPJob job = (YARNBSPJob) normalJob;

    LOG.info("Submitting job...");
    if (getConf().get("bsp.child.mem.in.mb") == null) {
      LOG.warn("BSP Child memory has not been set, YARN will guess your needs or use default values.");
    }

    if (fs == null) {
      fs = FileSystem.get(getConf());
    }

    if (getConf().get("bsp.user.name") == null) {
      String s = EnvironmentUtil.getUnixUserName();
      getConf().set("bsp.user.name", s);
      LOG.info("Retrieved username: " + s);
    }

    GetNewApplicationRequest request = Records
        .newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = job.getApplicationsManager()
        .getNewApplication(request);
    id = response.getApplicationId();
    LOG.info("Got new ApplicationId=" + id);

    // Create a new ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);
    // set the ApplicationId
    appContext.setApplicationId(this.id);
    // set the application name
    appContext.setApplicationName(job.getJobName());

    // Create a new container launch context for the AM's container
    ContainerLaunchContext amContainer = Records
        .newRecord(ContainerLaunchContext.class);

    // Define the local resources required
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    // Lets assume the jar we need for our ApplicationMaster is available in
    // HDFS at a certain known path to us and we want to make it available to
    // the ApplicationMaster in the launched container
    if (job.getJar() == null) {
      throw new IllegalArgumentException(
          "Jar must be set in order to run the application!");
    }
    Path jarPath = new Path(job.getWorkingDirectory(), id + "/app.jar");
    fs.copyFromLocalFile(job.getLocalPath(job.getJar()), jarPath);
    LOG.info("Copying app jar to " + jarPath);
    getConf()
        .set(
            "bsp.jar",
            jarPath.makeQualified(fs.getUri(), fs.getWorkingDirectory())
                .toString());
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

    // Construct the command to be executed on the launched container
    String command = "${JAVA_HOME}"
        + "/bin/java -cp "
        + classPathEnv
        + " "
        + BSPApplicationMaster.class.getCanonicalName()
        + " "
        + submitJobFile.makeQualified(fs.getUri(), fs.getWorkingDirectory())
            .toString() + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
        + "/stderr";

    LOG.info("Start command: " + command);

    amContainer.setCommands(Collections.singletonList(command));

    Resource capability = Records.newRecord(Resource.class);
    // we have at least 3 threads, which comsumes 1mb each, for each bsptask and
    // a base usage of 100mb
    capability.setMemory(3 * job.getNumBspTask()
        + getConf().getInt("hama.appmaster.memory.mb", 100));
    LOG.info("Set memory for the application master to "
        + capability.getMemory() + "mb!");
    amContainer.setResource(capability);

    // Set the container launch content into the ApplicationSubmissionContext
    appContext.setAMContainerSpec(amContainer);

    // Create the request to send to the ApplicationsManager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    job.getApplicationsManager().submitApplication(appRequest);

    GetApplicationReportRequest reportRequest = Records
        .newRecord(GetApplicationReportRequest.class);
    reportRequest.setApplicationId(id);
    while (report == null || report.getHost().equals("N/A")) {
      GetApplicationReportResponse reportResponse = job
          .getApplicationsManager().getApplicationReport(reportRequest);
      report = reportResponse.getApplicationReport();
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        LOG.error(
            "Got interrupted while waiting for a response report from AM.", e);
      }
    }
    LOG.info("Got report: " + report.getApplicationId() + " "
        + report.getHost());
    return new NetworkedJob();
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

}
