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
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Callable;

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
import org.apache.hama.bsp.BSPTaskLauncher.BSPTaskStatus;

public class BSPTaskLauncher implements Callable<BSPTaskStatus> {

  private static final Log LOG = LogFactory.getLog(BSPTaskLauncher.class);

  private final Container allocatedContainer;
  private final int id;
  private final ContainerManager cm;
  private final Configuration conf;
  private final String user;
  private final Path jobFile;
  private final BSPJobID jobId;

  public BSPTaskLauncher(int id, Container container, ContainerManager cm,
      Configuration conf, Path jobFile, BSPJobID jobId)
      throws YarnRemoteException {
    this.id = id;
    this.cm = cm;
    this.conf = conf;
    this.allocatedContainer = container;
    this.jobFile = jobFile;
    this.jobId = jobId;
    this.user = conf.get("bsp.user.name");
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

  @Override
  public BSPTaskStatus call() throws Exception {
    LOG.info("Spawned task with id: " + this.id
        + " for allocated container id: "
        + this.allocatedContainer.getId().toString());
    final GetContainerStatusRequest statusRequest = setupContainer(
        allocatedContainer, cm, user, id);

    ContainerStatus lastStatus;
    while ((lastStatus = cm.getContainerStatus(statusRequest).getStatus())
        .getState() != ContainerState.COMPLETE) {
      Thread.sleep(1000l);
    }

    return new BSPTaskStatus(id, lastStatus.getExitStatus());
  }

  private GetContainerStatusRequest setupContainer(
      Container allocatedContainer, ContainerManager cm, String user, int id)
      throws IOException {
    LOG.info("Setting up a container for user " + user + " with id of " + id
        + " and containerID of " + allocatedContainer.getId());
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
    URL packageUrl = ConverterUtils.getYarnUrlFromPath(packageFile);

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
