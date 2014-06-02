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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hama.ipc.GroomProtocol;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

public class TaskDelegator implements GroomStatusListener {
  public static final Log LOG = LogFactory.getLog(MesosScheduler.class);

  private Set<TaskInProgress> executingTasks;

  
  /**
   * Map to hold assignments from groomServerNames to TasksInProgress
   */
  private MultiValueMap assignments = new MultiValueMap();

  private AtomicReference<GroomServerManager> groomServerManager;

  /**
   * Map from Pair of groomServerName and port number to the latest
   * GroomServerStatus
   */
  private Map<Pair<String, Integer>, GroomServerStatus> groomServers = new HashMap<Pair<String, Integer>, GroomServerStatus>();

  private Map<Pair<String, Integer>, Protos.TaskID> groomTaskIDs = new HashMap<Pair<String, Integer>, Protos.TaskID>();

  private SchedulerDriver driver;

  public TaskDelegator(AtomicReference<GroomServerManager> groomServerManager,
      SchedulerDriver driver, Set<TaskInProgress> executingTasks) {
    this.groomServerManager = groomServerManager;
    this.driver = driver;
    this.executingTasks = executingTasks;
  }

  @Override
  public void groomServerRegistered(GroomServerStatus status) {
    Pair<String, Integer> key = new Pair<String, Integer>(
        status.getGroomHostName(), BSPMaster.resolveWorkerAddress(
            status.rpcServer).getPort());
    LOG.debug("Received Groom From: " + key.getKey() + ":" + key.getValue());

    if (assignments.containsKey(key)) {
      for (Object tip : assignments.getCollection(key)) {
        execute((TaskInProgress) tip, status);
      }
    } else {
      LOG.error("Unexpected host found: " + key.getKey() + ":" + key.getValue());
    }
    groomServers.put(key, status);
  }

  /**
   * Add a task for execution when the groom server becomes available
   * 
   * @param tip
   *          The TaskInProgress to execute
   * @param hostName
   *          The hostname where the resource reservation was made
   */
  public void addTask(TaskInProgress tip, Protos.TaskID taskId,
      String hostName, Integer port) {
    LOG.trace("Adding Host: " + hostName + ":" + port + " for Task:"
        + tip.getTaskId());
    Pair<String, Integer> key = new Pair<String, Integer>(hostName, port);

    groomTaskIDs.put(key, taskId);

    if (groomServers.containsKey(key)) {
      execute(tip, groomServers.get(key));
    } else {
      assignments.put(key, tip);
    }
  }

  private void execute(TaskInProgress tip, GroomServerStatus status) {
    Task task = tip.constructTask(status);

    GroomProtocol worker = groomServerManager.get().findGroomServer(status);

    GroomServerAction[] actions = new GroomServerAction[1];
    actions[0] = new LaunchTaskAction(task);
    Directive d1 = new DispatchTasksDirective(actions);
    try {
      worker.dispatch(d1);
    } catch (IOException ioe) {
      LOG.error(
          "Fail to dispatch tasks to GroomServer " + status.getGroomName(), ioe);
    }
  }

  @Override
  public void taskComplete(GroomServerStatus status, TaskInProgress task) {
    Pair<String, Integer> key = new Pair<String, Integer>(
        status.getGroomHostName(), BSPMaster.resolveWorkerAddress(
            status.rpcServer).getPort());
    groomServers.put(key, status);
    assignments.remove(key, task);
    
    if (assignments.getCollection(key) == null) {
      groomServers.remove(key);
      driver.killTask(groomTaskIDs.get(key));
    }
  }
}
