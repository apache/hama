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

import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 */
class TaskInProgress {
  public static final Log LOG = LogFactory.getLog(TaskInProgress.class);

  private BSPJobContext context;

  // Constants
  static final int MAX_TASK_EXECS = 1;
  int maxTaskAttempts = 4;

  // Job Meta
  private String jobFile = null;
  private int partition;
  private BSPMaster bspMaster;
  private TaskID id;
  private JobInProgress job;
  private int completes = 0;

  // Status
  // private double progress = 0;
  // private String state = "";
  private long startTime = 0;

  // The 'next' usable taskid of this tip
  int nextTaskId = 0;

  // The taskid that took this TIP to SUCCESS
  // private TaskAttemptID successfulTaskId;

  // The first taskid of this tip
  private TaskAttemptID firstTaskId;

  // Map from task Id -> GroomServer Id, contains tasks that are
  // currently runnings
  private TreeMap<TaskAttemptID, String> activeTasks = new TreeMap<TaskAttemptID, String>();
  // All attempt Ids of this TIP
  // private TreeSet<TaskAttemptID> tasks = new TreeSet<TaskAttemptID>();
  /**
   * Map from taskId -> TaskStatus
   */
  private TreeMap<TaskAttemptID, TaskStatus> taskStatuses = new TreeMap<TaskAttemptID, TaskStatus>();

  public TaskInProgress(BSPJobID jobId, String jobFile, BSPMaster master,
      BSPJobContext context, JobInProgress job, int partition) {
    this.jobFile = jobFile;
    this.bspMaster = master;
    this.job = job;
    this.context = context;
    this.partition = partition;
  }

  // //////////////////////////////////
  // Accessors
  // //////////////////////////////////
  /**
   * Return the start time
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Return the parent job
   */
  public JobInProgress getJob() {
    return job;
  }

  public TaskID getTIPId() {
    return this.id;
  }

  /**
   * Is the Task associated with taskid is the first attempt of the tip?
   * 
   * @param taskId
   * @return Returns true if the Task is the first attempt of the tip
   */
  public boolean isFirstAttempt(TaskAttemptID taskId) {
    return firstTaskId == null ? false : firstTaskId.equals(taskId);
  }

  /**
   * Is this tip currently running any tasks?
   * 
   * @return true if any tasks are running
   */
  public boolean isRunning() {
    return !activeTasks.isEmpty();
  }

  /**
   * Is this tip complete?
   * 
   * @return <code>true</code> if the tip is complete, else <code>false</code>
   */
  public synchronized boolean isComplete() {
    return (completes > 0);
  }
}
