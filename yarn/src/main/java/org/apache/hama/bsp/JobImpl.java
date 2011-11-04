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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.bsp.BSPTaskLauncher.BSPTaskStatus;

public class JobImpl implements Job {

  private static final Log LOG = LogFactory.getLog(JobImpl.class);
  private static final ExecutorService threadPool = Executors
      .newCachedThreadPool();

  private static final int DEFAULT_MEMORY_MB = 256;

  private Configuration conf;
  private BSPJobID jobId;
  private int numBSPTasks;
  private int priority = 0;
  private String childOpts;
  private int taskMemoryInMb;
  private Path jobFile;

  private JobState state;
  private BSPPhase phase;

  private ApplicationAttemptId appAttemptId;
  private YarnRPC yarnRPC;
  private AMRMProtocol resourceManager;

  private List<Container> allocatedContainers;
  private List<ContainerId> releasedContainers = Collections.emptyList();

  private ExecutorCompletionService<BSPTaskStatus> completionService = new ExecutorCompletionService<BSPTaskStatus>(
      threadPool);
  private Map<Integer, BSPTaskLauncher> launchers = new HashMap<Integer, BSPTaskLauncher>();
  private int lastResponseID = 0;

  public JobImpl(ApplicationAttemptId appAttemptId,
      Configuration jobConfiguration, YarnRPC yarnRPC, AMRMProtocol amrmRPC,
      String jobFile, BSPJobID jobId) {
    super();
    this.numBSPTasks = jobConfiguration.getInt("bsp.peers.num", 1);
    this.appAttemptId = appAttemptId;
    this.yarnRPC = yarnRPC;
    this.resourceManager = amrmRPC;
    this.jobFile = new Path(jobFile);
    this.state = JobState.NEW;
    this.jobId = jobId;
    this.conf = jobConfiguration;
    this.childOpts = conf.get("bsp.child.java.opts");

    this.taskMemoryInMb = getMemoryRequirements();
    LOG.info("Memory per task: " + taskMemoryInMb + "m!");
  }

  private int getMemoryRequirements() {
    String newMemoryProperty = conf.get("bsp.child.mem.in.mb");
    if (newMemoryProperty == null) {
      LOG.warn("\"bsp.child.mem.in.mb\" was not set! Try parsing the child opts...");
      return getMemoryFromOptString(childOpts);
    } else {
      return Integer.valueOf(newMemoryProperty);
    }
  }

  // This really needs a testcase
  private int getMemoryFromOptString(String opts) {
    if (!opts.contains("-Xmx")) {
      LOG.info("No \"-Xmx\" option found in child opts, using default amount of memory!");
      return DEFAULT_MEMORY_MB;
    } else {
      // e.G: -Xmx512m
      int startIndex = opts.indexOf("-Xmx") + 4;
      int endIndex = opts.indexOf(" ", startIndex);
      String xmxString = opts.substring(startIndex, endIndex);
      char qualifier = xmxString.charAt(xmxString.length() - 1);
      int memory = Integer.valueOf(xmxString.substring(0,
          xmxString.length() - 2));
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

  @Override
  public JobState startJob() throws YarnRemoteException, InterruptedException,
      ExecutionException {

    this.allocatedContainers = new ArrayList<Container>(numBSPTasks);
    while (allocatedContainers.size() < numBSPTasks) {

      AllocateRequest req = BuilderUtils.newAllocateRequest(
          appAttemptId,
          lastResponseID,
          0.0f,
          createBSPTaskRequest(numBSPTasks - allocatedContainers.size(),
              taskMemoryInMb, priority), releasedContainers);

      AllocateResponse allocateResponse = resourceManager.allocate(req);
      AMResponse amResponse = allocateResponse.getAMResponse();
      LOG.info("Got response! ID: " + amResponse.getResponseId()
          + " with num of containers: "
          + amResponse.getAllocatedContainers().size()
          + " and following resources: "
          + amResponse.getAvailableResources().getMemory() + "mb");
      this.lastResponseID = amResponse.getResponseId();

//      availableResources = amResponse.getAvailableResources();
      this.allocatedContainers.addAll(amResponse.getAllocatedContainers());
      LOG.info("Waiting to allocate "
          + (numBSPTasks - allocatedContainers.size()) + " more containers...");
      Thread.sleep(1000l);
    }

    LOG.info("Got " + allocatedContainers.size() + " containers!");

    int launchedBSPTasks = 0;

    int id = 0;
    for (Container allocatedContainer : allocatedContainers) {
      LOG.info("Launching task on a new container." + ", containerId="
          + allocatedContainer.getId() + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress() + ", containerState"
          + allocatedContainer.getState() + ", containerResourceMemory"
          + allocatedContainer.getResource().getMemory());

      // Connect to ContainerManager on the allocated container
      String cmIpPortStr = allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort();
      InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
      ContainerManager cm = (ContainerManager) yarnRPC.getProxy(
          ContainerManager.class, cmAddress, conf);

      BSPTaskLauncher runnableLaunchContainer = new BSPTaskLauncher(id,
          allocatedContainer, cm, conf, jobFile, jobId);
      launchers.put(id, runnableLaunchContainer);
      completionService.submit(runnableLaunchContainer);
      id++;
      launchedBSPTasks++;
    }
    state = JobState.RUNNING;

    for (int i = 0; i < launchedBSPTasks; i++) {
      BSPTaskStatus returnedTask = completionService.take().get();
      if (returnedTask.getExitStatus() != 0) {
        LOG.error("Task with id \"" + returnedTask.getId() + "\" failed!");
        state = JobState.FAILED;
        return state;
      } else {
        LOG.info("Task \"" + returnedTask.getId() + "\" sucessfully finished!");
      }
      cleanupTask(returnedTask.getId());
    }

    state = JobState.SUCCESS;
    return state;
  }

  /**
   * Makes a lookup for the taskid and stops its container and task. It also
   * removes the task from the launcher so that we won't have to stop it twice.
   * 
   * @param id
   * @throws YarnRemoteException
   */
  private void cleanupTask(int id) throws YarnRemoteException {
    BSPTaskLauncher bspTaskLauncher = launchers.get(id);
    bspTaskLauncher.stopAndCleanup();
    launchers.remove(id);
  }

  @Override
  public void cleanup() throws YarnRemoteException {
    for (BSPTaskLauncher launcher : launchers.values()) {
      launcher.stopAndCleanup();
    }
    threadPool.shutdownNow();
  }

  private List<ResourceRequest> createBSPTaskRequest(int numTasks,
      int memoryInMb, int priority) {

    List<ResourceRequest> reqList = new ArrayList<ResourceRequest>(numTasks);
    for (int i = 0; i < numTasks; i++) {
      // Resource Request
      ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);

      // setup requirements for hosts
      // whether a particular rack/host is needed
      // useful for applications that are sensitive
      // to data locality
      rsrcRequest.setHostName("*");

      // set the priority for the request
      Priority pri = Records.newRecord(Priority.class);
      pri.setPriority(priority);
      rsrcRequest.setPriority(pri);

      // Set up resource type requirements
      // For now, only memory is supported so we set memory requirements
      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(memoryInMb);
      rsrcRequest.setCapability(capability);

      // set no. of containers needed
      // matching the specifications
      rsrcRequest.setNumContainers(numBSPTasks);
      reqList.add(rsrcRequest);
    }
    return reqList;
  }

  @Override
  public JobState getState() {
    return state;
  }

  @Override
  public int getTotalBSPTasks() {
    return numBSPTasks;
  }

  @Override
  public BSPPhase getBSPPhase() {
    return phase;
  }

}
