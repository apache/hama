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
import java.net.URISyntaxException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class BSPTaskLauncher {

  private static final Log LOG = LogFactory.getLog(BSPTaskLauncher.class);

  private final Container allocatedContainer;
  private final int id;
  private final ContainerManagementProtocol cm;
  private final Configuration conf;
  private String user;
  private final Path jobFile;
  private final BSPJobID jobId;

  private GetContainerStatusesRequest statusRequest;
  
  @Override
  protected void finalize() throws Throwable {
    stopAndCleanup();
  }

  public BSPTaskLauncher(int id, Container container, ContainerManagementProtocol cm,
                         Configuration conf, Path jobFile, BSPJobID jobId)
      throws YarnException {
    this.id = id;
    this.cm = cm;
    this.conf = conf;
    this.allocatedContainer = container;
    this.jobFile = jobFile;
    this.jobId = jobId;
    // FIXME why does this contain mapreduce here?
    this.user = conf.get("bsp.user.name");
    if (this.user == null) {
      this.user = conf.get("mapreduce.job.user.name");
    }
  }

  public void stopAndCleanup() throws YarnException, IOException {
    StopContainersRequest stopRequest = Records.newRecord(StopContainersRequest.class);
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(allocatedContainer.getId());
    LOG.info("getId : " + allocatedContainer.getId());
    stopRequest.setContainerIds(containerIds);
    LOG.info("StopContainer : " + stopRequest.getContainerIds());
    cm.stopContainers(stopRequest);

  }

  public void start() throws IOException, YarnException {
    LOG.info("Spawned task with id: " + this.id
        + " for allocated container id: "
        + this.allocatedContainer.getId().toString());
    statusRequest = setupContainer(allocatedContainer, cm, user, id);
  }

  /**
   * This polls the current container status from container manager. Null if the
   * container hasn't finished yet.
   * 
   * @return
   * @throws Exception
   */
  public BSPTaskStatus poll() throws Exception {

    ContainerStatus lastStatus = null;
    GetContainerStatusesResponse getContainerStatusesResponse = cm.getContainerStatuses(statusRequest);
    List<ContainerStatus> containerStatuses = getContainerStatusesResponse.getContainerStatuses();
    for (ContainerStatus containerStatus : containerStatuses) {
      LOG.info("Got container status for containerID="
          + containerStatus.getContainerId() + ", state="
          + containerStatus.getState() + ", exitStatus="
          + containerStatus.getExitStatus() + ", diagnostics="
          + containerStatus.getDiagnostics());

      if (containerStatus.getContainerId().equals(allocatedContainer.getId())) {
        lastStatus = containerStatus;
        break;
      }
    }
    if (lastStatus.getState() != ContainerState.COMPLETE) {
      return null;
    }
    LOG.info(this.id + " Last report comes with exitstatus of "
        + lastStatus.getExitStatus() + " and diagnose string of "
        + lastStatus.getDiagnostics());

    return new BSPTaskStatus(id, lastStatus.getExitStatus());
  }

  private GetContainerStatusesRequest setupContainer(
      Container allocatedContainer, ContainerManagementProtocol cm, String user, int id) throws IOException, YarnException {
    LOG.info("Setting up a container for user " + user + " with id of " + id
        + " and containerID of " + allocatedContainer.getId() + " as " + user);
    // Now we setup a ContainerLaunchContext
    ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);

    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    LocalResource packageResource = Records.newRecord(LocalResource.class);
    FileSystem fs = FileSystem.get(conf);
    Path packageFile = new Path(System.getenv(YARNBSPConstants.HAMA_YARN_LOCATION));
    URL packageUrl = ConverterUtils.getYarnUrlFromPath(packageFile
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()));
    LOG.info("PackageURL has been composed to " + packageUrl.toString());
    try {
      LOG.info("Reverting packageURL to path: "
          + ConverterUtils.getPathFromYarnURL(packageUrl));
    } catch (URISyntaxException e) {
      LOG.fatal("If you see this error the workarround does not work", e);
    }

    packageResource.setResource(packageUrl);
    packageResource.setSize(Long.parseLong(System.getenv(YARNBSPConstants.HAMA_YARN_SIZE)));
    packageResource.setTimestamp(Long.parseLong(System.getenv(YARNBSPConstants.HAMA_YARN_TIMESTAMP)));
    packageResource.setType(LocalResourceType.FILE);
    packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

    localResources.put(YARNBSPConstants.APP_MASTER_JAR_PATH, packageResource);

    Path hamaReleaseFile = new Path(System.getenv(YARNBSPConstants.HAMA_RELEASE_LOCATION));
    URL hamaReleaseUrl = ConverterUtils.getYarnUrlFromPath(hamaReleaseFile
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()));
    LOG.info("Hama release URL has been composed to " + hamaReleaseUrl.toString());

    LocalResource hamaReleaseRsrc = Records.newRecord(LocalResource.class);
    hamaReleaseRsrc.setResource(hamaReleaseUrl);
    hamaReleaseRsrc.setSize(Long.parseLong(System.getenv(YARNBSPConstants.HAMA_RELEASE_SIZE)));
    hamaReleaseRsrc.setTimestamp(Long.parseLong(System.getenv(YARNBSPConstants.HAMA_RELEASE_TIMESTAMP)));
    hamaReleaseRsrc.setType(LocalResourceType.ARCHIVE);
    hamaReleaseRsrc.setVisibility(LocalResourceVisibility.APPLICATION);

    localResources.put(YARNBSPConstants.HAMA_SYMLINK, hamaReleaseRsrc);

    ctx.setLocalResources(localResources);

    /*
     * TODO Package classpath seems not to work if you're in pseudo distributed
     * mode, because the resource must not be moved, it will never be unpacked.
     * So we will check if our jar file has the file:// prefix and put it into
     * the CP directly
     */

    StringBuilder classPathEnv = new StringBuilder(
        ApplicationConstants.Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
        .append("./*");
    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }

    classPathEnv.append(File.pathSeparator);
    classPathEnv.append("./" + YARNBSPConstants.HAMA_SYMLINK +
        "/" + YARNBSPConstants.HAMA_RELEASE_VERSION +  "/*");
    classPathEnv.append(File.pathSeparator);
    classPathEnv.append("./" + YARNBSPConstants.HAMA_SYMLINK +
        "/" + YARNBSPConstants.HAMA_RELEASE_VERSION + "/lib/*");

    Vector<CharSequence> vargs = new Vector<CharSequence>();
    vargs.add("${JAVA_HOME}/bin/java");
    vargs.add("-cp " + classPathEnv + "");
    vargs.add(BSPRunner.class.getCanonicalName());
    
    vargs.add(jobId.getJtIdentifier());
    vargs.add(Integer.toString(id));
    vargs.add(this.jobFile.makeQualified(fs.getUri(), fs.getWorkingDirectory())
        .toString());

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/bsp.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/bsp.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());

    ctx.setCommands(commands);
    LOG.info("Starting command: " + commands);

    StartContainerRequest startReq = Records
        .newRecord(StartContainerRequest.class);
    startReq.setContainerLaunchContext(ctx);
    startReq.setContainerToken(allocatedContainer.getContainerToken());

    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(startReq);
    StartContainersRequest requestList = StartContainersRequest.newInstance(list);
    cm.startContainers(requestList);

    GetContainerStatusesRequest statusReq = Records
        .newRecord(GetContainerStatusesRequest.class);
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(allocatedContainer.getId());
    statusReq.setContainerIds(containerIds);
    return statusReq;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BSPTaskLauncher other = (BSPTaskLauncher) obj;
    if (id != other.id)
      return false;
    return true;
  }

  public static class BSPTaskStatus {
    private final int id;
    private final int exitStatus;

    public BSPTaskStatus(int id, int exitStatus) {
      super();
      this.id = id;
      this.exitStatus = exitStatus;
    }

    public int getId() {
      return id;
    }

    public int getExitStatus() {
      return exitStatus;
    }
  }

}
