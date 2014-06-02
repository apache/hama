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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.ipc.GroomProtocol;

/**
 * A simple taskWorkerManager that assumes groom servers are running
 * and may be assigned jobs
 */
public class SimpleTaskWorkerManager extends TaskWorkerManager {
  private static final Log LOG = LogFactory
      .getLog(SimpleTaskWorkerManager.class);

  @Override
  public TaskWorker spawnWorker(JobInProgress jip) {
    Collection<GroomServerStatus> statusCollection = groomServerManager.get()
        .groomServerStatusKeySet();

    ClusterStatus clusterStatus = groomServerManager.get()
        .getClusterStatus(false);
    final int numGroomServers = clusterStatus.getGroomServers();

    return new SimpleTaskWorker(statusCollection, numGroomServers, jip);
  }

  private class SimpleTaskWorker implements TaskWorker {
    private final Map<String, GroomServerStatus> groomStatuses;
    private final int groomNum;
    private final JobInProgress jip;

    SimpleTaskWorker(final Collection<GroomServerStatus> stus,
        final int num, final JobInProgress jip) {
      this.groomStatuses = new HashMap<String, GroomServerStatus>(2 * num);
      for (GroomServerStatus status : stus) {
        this.groomStatuses.put(status.hostName, status);
      }
      this.groomNum = num;
      this.jip = jip;
      if (null == this.groomStatuses)
        throw new NullPointerException("Target groom server is not "
            + "specified.");
      if (-1 == this.groomNum)
        throw new IllegalArgumentException(
            "Groom number is not specified.");
      if (null == this.jip)
        throw new NullPointerException("No job is specified.");
    }

    private Boolean scheduleNewTasks() {

      // Action to be sent for each task to the respective groom server.
      Map<GroomServerStatus, List<GroomServerAction>> actionMap = new HashMap<GroomServerStatus, List<GroomServerAction>>(
          2 * this.groomStatuses.size());
      Set<Task> taskSet = new HashSet<Task>(2 * jip.tasks.length);
      Task t = null;
      int cnt = 0;
      while ((t = jip.obtainNewTask(this.groomStatuses)) != null) {
        taskSet.add(t);
        // Scheduled all tasks
        if (++cnt == this.jip.tasks.length) {
          break;
        }
      }

      // if all tasks could not be scheduled
      if (cnt != this.jip.tasks.length) {
        LOG.error("Could not schedule all tasks!");
        return Boolean.FALSE;
      }

      // assembly into actions
      for (Task task : taskSet) {
        GroomServerStatus groomStatus = jip.getGroomStatusForTask(task);
        List<GroomServerAction> taskActions = actionMap
            .get(groomStatus);
        if (taskActions == null) {
          taskActions = new ArrayList<GroomServerAction>(
              groomStatus.getMaxTasks());
        }
        taskActions.add(new LaunchTaskAction(task));
        actionMap.put(groomStatus, taskActions);
      }

      sendDirectivesToGrooms(actionMap);

      return Boolean.TRUE;
    }

    /**
     * Schedule recovery tasks.
     * 
     * @return TRUE object if scheduling is successful else returns FALSE
     */
    private Boolean scheduleRecoveryTasks() {

      // Action to be sent for each task to the respective groom server.
      Map<GroomServerStatus, List<GroomServerAction>> actionMap = new HashMap<GroomServerStatus, List<GroomServerAction>>(
          2 * this.groomStatuses.size());

      try {
        jip.recoverTasks(groomStatuses, actionMap);
      } catch (IOException e) {
        return Boolean.FALSE;
      }
      return sendDirectivesToGrooms(actionMap);

    }

    private Boolean sendDirectivesToGrooms(
        Map<GroomServerStatus, List<GroomServerAction>> actionMap) {
      Iterator<GroomServerStatus> groomIter = actionMap.keySet()
          .iterator();
      while ((jip.getStatus().getRunState() == JobStatus.RUNNING || jip
          .getStatus().getRunState() == JobStatus.RECOVERING)
          && groomIter.hasNext()) {

        GroomServerStatus groomStatus = groomIter.next();
        List<GroomServerAction> actionList = actionMap.get(groomStatus);

        GroomProtocol worker = groomServerManager.get()
            .findGroomServer(groomStatus);
        try {
          // dispatch() to the groom server
          GroomServerAction[] actions = new GroomServerAction[actionList
                                                              .size()];
          actionList.toArray(actions);
          Directive d1 = new DispatchTasksDirective(actions);
          worker.dispatch(d1);
        } catch (IOException ioe) {
          LOG.error("Fail to dispatch tasks to GroomServer "
              + groomStatus.getGroomName(), ioe);
          return Boolean.FALSE;
        }

      }

      if (groomIter.hasNext()
          && (jip.getStatus().getRunState() != JobStatus.RUNNING || jip
          .getStatus().getRunState() != JobStatus.RECOVERING)) {
        LOG.warn("Currently master only shcedules job in running state. "
            + "This may be refined in the future. JobId:"
            + jip.getJobID());
        return Boolean.FALSE;
      }

      return Boolean.TRUE;
    }

    @Override
    public Boolean call() {
      if (jip.isRecoveryPending()) {
        return scheduleRecoveryTasks();
      } else {
        return scheduleNewTasks();
      }
    }
  }

}
