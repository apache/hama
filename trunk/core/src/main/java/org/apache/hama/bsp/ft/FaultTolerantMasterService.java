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
package org.apache.hama.bsp.ft;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hama.bsp.GroomServerAction;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.JobInProgress;
import org.apache.hama.bsp.TaskInProgress;

/**
 * <code>FaultTolerantMasterService</code> defines the behavior of object
 * responsible for doing fault-tolerance related work on BSPMaster task
 * scheduler. This is defined per job.
 */
public interface FaultTolerantMasterService {

  /**
   * Returns true if recovery of the task in question is possible.
   * 
   * @param tip <code>TaskInProgress</code> object that represents the task.
   * @return true if recovery is possible.
   */
  public boolean isRecoveryPossible(TaskInProgress tip);

  /**
   * Returns true if the task is already slated to be recovered for failure.
   * 
   * @param tip <code>TaskInProgress</code> object that represents the task.
   * @return if task/job is already in process of recovery.
   */
  public boolean isAlreadyRecovered(TaskInProgress tip);

  /**
   * From the list of tasks that are failed, provide the task scheduler a set of
   * actions and the grooms to which these actions must be sent for fault
   * recovery.
   * 
   * @param jip The job in question which has to be recovered.
   * @param groomStatuses The map of grooms to their statuses.
   * @param failedTasksInProgress The list of failed tasks.
   * @param allTasksInProgress This list of all tasks in the job.
   * @param actionMap The map of groom to the list of actions that are to be
   *          taken on that groom.
   */
  public void recoverTasks(JobInProgress jip,
      Map<String, GroomServerStatus> groomStatuses,
      TaskInProgress[] failedTasksInProgress,
      TaskInProgress[] allTasksInProgress,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      Map<GroomServerStatus, List<GroomServerAction>> actionMap)
      throws IOException;
}
