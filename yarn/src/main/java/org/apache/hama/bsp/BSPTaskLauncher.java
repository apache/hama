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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class BSPTaskLauncher {

  private static final Log LOG = LogFactory.getLog(BSPTaskLauncher.class);

  private final Container allocatedContainer;
  private final int id;
  private final ContainerManager cm;
  private final Configuration conf;
  private String user;
  private final Path jobFile;
  private final BSPJobID jobId;

  private GetContainerStatusRequest statusRequest;

  public BSPTaskLauncher(int id, Container container, ContainerManager cm,
      Configuration conf, Path jobFile, BSPJobID jobId)
      throws YarnRemoteException {
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

  @Override
  protected void finalize() throws Throwable {
    stopAndCleanup();
  }

  public void stopAndCleanup() throws YarnRemoteException {
    StopContainerRequest stopRequest = Records
        .newRecord(StopContainerRequest.class);
    stopRequest.setContainerId(allocatedContainer.getId());
    cm.stopContainer(stopRequest);
  }

  public void start() throws IOException {
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

    ContainerStatus lastStatus;
    if ((lastStatus = cm.getContainerStatus(statusRequest).getStatus())
        .getState() != ContainerState.COMPLETE) {
      return null;
    }
    LOG.info(this.id + "\tLast report comes with existatus of "
        + lastStatus.getExitStatus() + " and diagnose string of "
        + lastStatus.getDiagnostics());
    return new BSPTaskStatus(id, lastStatus.getExitStatus());
  }

  private GetContainerStatusRequest setupContainer(
      Container allocatedContainer, ContainerManager cm, String user, int id)
      throws IOException {
    LOG.info("Setting up a container for user " + user + " with id of " + id
        + " and containerID of " + allocatedContainer.getId() + " as " + user);
    // Now we setup a ContainerLaunchContext
    ContainerLaunchContext ctx = Records
        .newRecord(ContainerLaunchContext.class);

    ctx.setContainerId(allocatedContainer.getId());
    ctx.setResource(allocatedContainer.getResource());
    ctx.setUser(user);

    /*
     * jar
     */
    LocalResource packageResource = Records.newRecord(LocalResource.class);
    FileSystem fs = FileSystem.get(conf);
    Path packageFile = new Path(conf.get("bsp.jar"));
    // FIXME there seems to be a problem with the converter utils and URL
    // transformation
    URL packageUrl = ConverterUtils.getYarnUrlFromPath(packageFile
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()));
    LOG.info("PackageURL has been composed to " + packageUrl.toString());
    try {
      LOG.info("Reverting packageURL to path: "
          + ConverterUtils.getPathFromYarnURL(packageUrl));
    } catch (URISyntaxException e) {
      LOG.fatal("If you see this error the workarround does not work", e);
    }

    FileStatus fileStatus = fs.getFileStatus(packageFile);
    packageResource.setResource(packageUrl);
    packageResource.setSize(fileStatus.getLen());
    packageResource.setTimestamp(fileStatus.getModificationTime());
    packageResource.setType(LocalResourceType.ARCHIVE);
    packageResource.setVisibility(LocalResourceVisibility.APPLICATION);
    LOG.info("Package resource: " + packageResource.getResource());

    ctx.setLocalResources(Collections.singletonMap("package", packageResource));

    /*
     * TODO Package classpath seems not to work if you're in pseudo distributed
     * mode, because the resource must not be moved, it will never be unpacked.
     * So we will check if our jar file has the file:// prefix and put it into
     * the CP directly
     */
    String cp = "$CLASSPATH:./*:./package/*:./*:";
    if (packageUrl.getScheme() != null && packageUrl.getScheme().equals("file")) {
      cp += packageFile.makeQualified(fs.getUri(), fs.getWorkingDirectory())
          .toString() + ":";
      LOG.info("Localized file scheme detected, adjusting CP to: " + cp);
    }
    String[] cmds = {
        "${JAVA_HOME}" + "/bin/java -cp \"" + cp + "\" "
            + BSPRunner.class.getCanonicalName(),
        jobId.getJtIdentifier(),
        id + "",
        this.jobFile.makeQualified(fs.getUri(), fs.getWorkingDirectory())
            .toString(),
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr" };
    ctx.setCommands(Arrays.asList(cmds));
    LOG.info("Starting command: " + Arrays.toString(cmds));

    StartContainerRequest startReq = Records
        .newRecord(StartContainerRequest.class);
    startReq.setContainerLaunchContext(ctx);
    cm.startContainer(startReq);

    GetContainerStatusRequest statusReq = Records
        .newRecord(GetContainerStatusRequest.class);
    statusReq.setContainerId(allocatedContainer.getId());
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
