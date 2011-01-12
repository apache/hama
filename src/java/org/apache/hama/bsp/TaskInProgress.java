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
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobStatus;

/**
 *
 */
class TaskInProgress {
  public static final Log LOG = LogFactory.getLog(TaskInProgress.class);

  private Configuration conf;

  // Constants
  static final int MAX_TASK_EXECS = 1;
  int maxTaskAttempts = 4;
  private boolean failed = false;
  private static final int NUM_ATTEMPTS_PER_RESTART = 1000;

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
  private TaskAttemptID successfulTaskId;

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

  private BSPJobID jobId;

  /**
   * Constructor for new nexus between BSPMaster and GroomServer.
   * 
   * @param jobId is identification of JobInProgress.
   * @param jobFile the path of job file
   * @param partition which partition this TaskInProgress owns.
   */
  public TaskInProgress(BSPJobID jobId, String jobFile, int partition) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.partition = partition;

    this.id = new TaskID(jobId, true, partition);
  }

  public TaskInProgress(BSPJobID jobId, String jobFile, BSPMaster master,
      Configuration conf, JobInProgress job, int partition) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.bspMaster = master;
    this.job = job;
    this.conf = conf;
    this.partition = partition;

    this.id = new TaskID(jobId, true, partition);
  }

  /**
   * Return a Task that can be sent to a GroomServer for execution.
   */
  public Task getTaskToRun(GroomServerStatus status) throws IOException {
    Task t = null;

    TaskAttemptID taskid = null;
    if (nextTaskId < (MAX_TASK_EXECS + maxTaskAttempts)) {
      int attemptId = job.getNumRestarts() * NUM_ATTEMPTS_PER_RESTART
          + nextTaskId;
      taskid = new TaskAttemptID(id, attemptId);
      ++nextTaskId;
    } else {
      LOG.warn("Exceeded limit of " + (MAX_TASK_EXECS + maxTaskAttempts)
          + " attempts for the tip '" + getTIPId() + "'");
      return null;
    }

    t = new BSPTask(jobId, jobFile, taskid, partition);
    activeTasks.put(taskid, status.getGroomName());

    return t;
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
    return id;
  }

  public TaskID getTaskId() {
    return this.id;
  }

  public TreeMap<TaskAttemptID, String> getTasks() {
    return activeTasks;
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

  /**
   * Is the given taskid the one that took this tip to completion?
   * 
   * @param taskid taskid of attempt to check for completion
   * @return <code>true</code> if taskid is complete, else <code>false</code>
   */
  public boolean isComplete(TaskAttemptID taskid) {
    return (completes > 0 && taskid.equals(getSuccessfulTaskid()));
  }

  private TreeSet<TaskAttemptID> tasksReportedClosed = new TreeSet<TaskAttemptID>();

  public boolean shouldCloseForClosedJob(TaskAttemptID taskid) {
    TaskStatus ts = (TaskStatus) taskStatuses.get(taskid);
    if ((ts != null) && (!tasksReportedClosed.contains(taskid))
        && (job.getStatus().getRunState() != JobStatus.RUNNING)) {
      tasksReportedClosed.add(taskid);
      return true;
    } else {
      return false;
    }
  }

  public void completed(TaskAttemptID taskid) {
    LOG.info("Task '" + taskid.getTaskID().toString() + "' has completed.");

    TaskStatus status = (TaskStatus) taskStatuses.get(taskid);
    status.setRunState(TaskStatus.State.SUCCEEDED);
    activeTasks.remove(taskid);

    // Note the successful taskid
    setSuccessfulTaskid(taskid);

    //
    // Now that the TIP is complete, the other speculative
    // subtasks will be closed when the owning groom server
    // reports in and calls shouldClose() on this object.
    //

    this.completes++;
  }

  private void setSuccessfulTaskid(TaskAttemptID taskid) {
    this.successfulTaskId = taskid;
  }

  private TaskAttemptID getSuccessfulTaskid() {
    return successfulTaskId;
  }

  public void updateStatus(TaskStatus status) {
    taskStatuses.put(status.getTaskId(), status);
  }

  public TaskStatus getTaskStatus(TaskAttemptID taskId) {
    return this.taskStatuses.get(taskId);
  }

  public void kill() {
    LOG.debug(">> TaskInProgress.kill() step.");
    this.failed = true;
  }

  public boolean isFailed() {
    return failed;
  }

}
