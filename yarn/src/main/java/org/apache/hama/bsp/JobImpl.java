/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * regarding copyright ownership.  The ASF licenses this file
 * distributed with this work for additional information
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
import java.security.PrivilegedAction;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hama.bsp.BSPTaskLauncher.BSPTaskStatus;

public class JobImpl implements Job {

  private static final Log LOG = LogFactory.getLog(JobImpl.class);
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
  private ApplicationMasterProtocol resourceManager;

  private List<Container> allocatedContainers;
  private List<ContainerId> releasedContainers = Collections.emptyList();

  private Map<Integer, BSPTaskLauncher> launchers = new HashMap<Integer, BSPTaskLauncher>();
  private Deque<BSPTaskLauncher> completionQueue = new LinkedList<BSPTaskLauncher>();

  private int lastResponseID = 0;

  private int getMemoryRequirements() {
    String newMemoryProperty = conf.get("bsp.child.mem.in.mb");
    if (newMemoryProperty == null) {
      LOG.warn("\"bsp.child.mem.in.mb\" was not set! Try parsing the child opts...");
      return getMemoryFromOptString(childOpts);
    } else {
      return Integer.valueOf(newMemoryProperty);
    }
  }

  public JobImpl(ApplicationAttemptId appAttemptId,
                 Configuration jobConfiguration, YarnRPC yarnRPC, ApplicationMasterProtocol amrmRPC,
                 String jobFile, BSPJobID jobId) {
    super();
    this.appAttemptId = appAttemptId;
    this.yarnRPC = yarnRPC;
    this.resourceManager = amrmRPC;
    this.jobFile = new Path(jobFile);
    this.state = JobState.NEW;
    this.jobId = jobId;
    this.conf = jobConfiguration;
    this.numBSPTasks = conf.getInt("bsp.peers.num", 1);
    this.childOpts = conf.get("bsp.child.java.opts");

    this.taskMemoryInMb = getMemoryRequirements();
  }

  // This really needs a testcase
  private static int getMemoryFromOptString(String opts) {
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

  @Override
  public JobState startJob() throws Exception {

    this.allocatedContainers = new ArrayList<Container>(numBSPTasks);
    NMTokenCache nmTokenCache = new NMTokenCache();
    while (allocatedContainers.size() < numBSPTasks) {
      AllocateRequest req = AllocateRequest.newInstance(lastResponseID, 0.0f,
          createBSPTaskRequest(numBSPTasks - allocatedContainers.size(), taskMemoryInMb,
              priority), releasedContainers, null);

      AllocateResponse allocateResponse = resourceManager.allocate(req);
      for (NMToken token : allocateResponse.getNMTokens()) {
        nmTokenCache.setToken(token.getNodeId().toString(), token.getToken());
      }

      LOG.info("Got response ID: " + allocateResponse.getResponseId()
          + " with num of containers: "
          + allocateResponse.getAllocatedContainers().size()
          + " and following resources: "
          + allocateResponse.getAvailableResources().getMemory() + "mb");
      this.lastResponseID = allocateResponse.getResponseId();

      this.allocatedContainers.addAll(allocateResponse.getAllocatedContainers());

      LOG.info("Waiting to allocate " + (numBSPTasks - allocatedContainers.size()) + " more containers...");

      Thread.sleep(1000l);
    }

    LOG.info("Got " + allocatedContainers.size() + " containers!");

    int id = 0;
    for (Container allocatedContainer : allocatedContainers) {
      LOG.info("Launching task on a new container." + ", containerId="
          + allocatedContainer.getId() + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
          + allocatedContainer.getResource().getMemory());

      // Connect to ContainerManager on the allocated container
      String user = conf.get("bsp.user.name");
      if (user == null) {
        user = System.getenv(ApplicationConstants.Environment.USER.name());
      }

      ContainerManagementProtocol cm = null;
      try {
        cm = getContainerManagementProtocolProxy(yarnRPC,
            nmTokenCache.getToken(allocatedContainer.getNodeId().toString()), allocatedContainer.getNodeId(), user);
      } catch (Exception e) {
        LOG.error("Failed to create ContainerManager...");
        if (cm != null)
          yarnRPC.stopProxy(cm, conf);
        e.printStackTrace();
      }

      BSPTaskLauncher runnableLaunchContainer = new BSPTaskLauncher(id,
          allocatedContainer, cm, conf, jobFile, jobId);

      launchers.put(id, runnableLaunchContainer);
      runnableLaunchContainer.start();
      completionQueue.add(runnableLaunchContainer);
      id++;
    }

    LOG.info("Waiting for tasks to finish...");
    state = JobState.RUNNING;
    int completed = 0;

    List<Integer> cleanupTasks = new ArrayList<Integer>();
    while (completed != numBSPTasks) {
      for (BSPTaskLauncher task : completionQueue) {
        BSPTaskStatus returnedTask = task.poll();
        // if our task returned with a finished state
        if (returnedTask != null) {
          if (returnedTask.getExitStatus() != 0) {
            LOG.error("Task with id \"" + returnedTask.getId() + "\" failed!");
            cleanupTask(returnedTask.getId());
            state = JobState.FAILED;
            return state;
          } else {
            LOG.info("Task \"" + returnedTask.getId()
                + "\" sucessfully finished!");
            completed++;
            LOG.info("Waiting for " + (numBSPTasks - completed)
                + " tasks to finish!");
          }
          cleanupTasks.add(returnedTask.getId());
        }
      }
      Thread.sleep(1000L);
    }

    for (Integer stopId : cleanupTasks) {
      cleanupTask(stopId);
    }

    state = JobState.SUCCESS;
    return state;
  }

  /**
   *
   * @param rpc
   * @param nmToken
   * @param nodeId
   * @param user
   * @return
   */
  protected ContainerManagementProtocol getContainerManagementProtocolProxy(
      final YarnRPC rpc, Token nmToken, NodeId nodeId, String user) {
    ContainerManagementProtocol proxy;
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    final InetSocketAddress addr =
        NetUtils.createSocketAddr(nodeId.getHost(), nodeId.getPort());
    if (nmToken != null) {
      ugi.addToken(ConverterUtils.convertFromYarn(nmToken, addr));
    }

    proxy = ugi
        .doAs(new PrivilegedAction<ContainerManagementProtocol>() {
          @Override
          public ContainerManagementProtocol run() {
            return (ContainerManagementProtocol) rpc.getProxy(
                ContainerManagementProtocol.class,
                addr, conf);
          }
        });
    return proxy;
  }

  /**
   * Makes a lookup for the taskid and stops its container and task. It also
   * removes the task from the launcher so that we won't have to stop it twice.
   * 
   * @param id
   * @throws YarnException
   */
  private void cleanupTask(int id) throws YarnException, IOException {
    BSPTaskLauncher bspTaskLauncher = launchers.get(id);
    bspTaskLauncher.stopAndCleanup();
    launchers.remove(id);
    completionQueue.remove(bspTaskLauncher);
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
      rsrcRequest.setResourceName("*");

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
  public void cleanup() throws YarnException, IOException {
    for (BSPTaskLauncher launcher : completionQueue) {
      launcher.stopAndCleanup();
    }
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
