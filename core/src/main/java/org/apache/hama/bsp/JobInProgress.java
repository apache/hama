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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;

/**
 * JobInProgress maintains all the info for keeping a Job on the straight and
 * narrow. It keeps its JobProfile and its latest JobStatus, plus a set of
 * tables for doing bookkeeping of its Tasks.ss
 */
class JobInProgress {

  /**
   * Used when the a kill is issued to a job which is initializing.
   */
  static class KillInterruptedException extends InterruptedException {
    private static final long serialVersionUID = 1L;

    public KillInterruptedException(String msg) {
      super(msg);
    }
  }

  public static enum JobCounter {
    LAUNCHED_TASKS
  }

  static final Log LOG = LogFactory.getLog(JobInProgress.class);
  boolean tasksInited = false;

  Configuration conf;
  JobProfile profile;
  JobStatus status;
  Path jobFile = null;
  Path localJobFile = null;
  Path localJarFile = null;

  private LocalFileSystem localFs;
  // Indicates how many times the job got restarted
  private int restartCount;

  long startTime;
  long launchTime;
  long finishTime;

  private String jobName;

  // private LocalFileSystem localFs;
  private BSPJobID jobId;
  final BSPMaster master;
  TaskInProgress tasks[] = new TaskInProgress[0];
  private long superstepCounter;

  private final Counters counters = new Counters();

  int numBSPTasks = 0;
  int clusterSize;
  String jobSplit;

  Map<Task, GroomServerStatus> taskToGroomMap;
  // Used only for scheduling!
  Map<GroomServerStatus, Integer> tasksInGroomMap;

  private int taskCompletionEventTracker = 0;

  public JobInProgress(BSPJobID jobId, Path jobFile, BSPMaster master,
      Configuration conf) throws IOException {
    this.conf = conf;
    this.jobId = jobId;
    this.localFs = FileSystem.getLocal(conf);
    this.jobFile = jobFile;
    this.master = master;

    this.taskToGroomMap = new HashMap<Task, GroomServerStatus>(2 * tasks.length);

    this.tasksInGroomMap = new HashMap<GroomServerStatus, Integer>();

    this.status = new JobStatus(jobId, null, 0L, 0L, JobStatus.State.PREP
        .value(), counters);
    this.startTime = System.currentTimeMillis();
    this.superstepCounter = 0;
    this.restartCount = 0;

    this.localJobFile = master.getLocalPath(BSPMaster.SUBDIR + "/" + jobId
        + ".xml");
    this.localJarFile = master.getLocalPath(BSPMaster.SUBDIR + "/" + jobId
        + ".jar");

    Path jobDir = master.getSystemDirectoryForJob(jobId);
    FileSystem fs = jobDir.getFileSystem(conf);
    fs.copyToLocalFile(jobFile, localJobFile);
    BSPJob job = new BSPJob(jobId, localJobFile.toString());
    this.jobSplit = job.getConf().get("bsp.job.split.file");

    this.numBSPTasks = job.getNumBspTask();
    this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>(
        numBSPTasks + 10);

    this.profile = new JobProfile(job.getUser(), jobId, jobFile.toString(), job
        .getJobName());

    this.setJobName(job.getJobName());

    status.setUsername(job.getUser());
    status.setStartTime(startTime);

    String jarFile = job.getJar();
    if (jarFile != null) {
      fs.copyToLocalFile(new Path(jarFile), localJarFile);
    }

  }

  public JobProfile getProfile() {
    return profile;
  }

  public JobStatus getStatus() {
    return status;
  }

  public synchronized long getLaunchTime() {
    return launchTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public int getNumOfTasks() {
    return tasks.length;
  }

  /**
   * @return the number of desired tasks.
   */
  public int desiredBSPTasks() {
    return numBSPTasks;
  }

  /**
   * @return The JobID of this JobInProgress.
   */
  public BSPJobID getJobID() {
    return jobId;
  }

  public synchronized TaskInProgress findTaskInProgress(TaskID id) {
    if (areTasksInited()) {
      for (TaskInProgress tip : tasks) {
        if (tip.getTaskId().equals(id)) {
          return tip;
        }
      }
    }
    return null;
  }

  public synchronized boolean areTasksInited() {
    return this.tasksInited;
  }

  public String toString() {
    return "jobName:" + profile.getJobName() + "\n" + "submit user:"
        + profile.getUser() + "\n" + "JobId:" + jobId + "\n" + "JobFile:"
        + jobFile + "\n";
  }

  // ///////////////////////////////////////////////////
  // Create/manage tasks
  // ///////////////////////////////////////////////////

  public synchronized void initTasks() throws IOException {
    if (tasksInited) {
      return;
    }

    Path sysDir = new Path(this.master.getSystemDir());
    FileSystem fs = sysDir.getFileSystem(conf);
    if (jobSplit != null) {
      DataInputStream splitFile = fs.open(new Path(this.jobSplit));

      BSPJobClient.RawSplit[] splits;
      try {
        splits = BSPJobClient.readSplitFile(splitFile);
      } finally {
        splitFile.close();
      }
      numBSPTasks = splits.length;
      LOG.info("num BSPTasks: " + numBSPTasks);

      // adjust number of BSP tasks to actual number of splits
      this.tasks = new TaskInProgress[numBSPTasks];
      for (int i = 0; i < numBSPTasks; i++) {
        tasks[i] = new TaskInProgress(getJobID(), this.jobFile.toString(),
            splits[i], this.master, this.conf, this, i);
      }
    } else {
      this.tasks = new TaskInProgress[numBSPTasks];
      for (int i = 0; i < numBSPTasks; i++) {
        tasks[i] = new TaskInProgress(getJobID(), this.jobFile.toString(),
            null, this.master, this.conf, this, i);
      }
    }

    // Update job status
    this.status = new JobStatus(this.status.getJobID(), this.profile.getUser(),
        0L, 0L, JobStatus.RUNNING, counters);

    // delete all nodes before start
    master.clearZKNodes();
    master.createJobRoot(this.getJobID().toString());

    tasksInited = true;
    LOG.info("Job is initialized.");
  }

  public Iterator<GroomServerStatus> getGroomsForTask() {
    return null;
  }

  public GroomServerStatus getGroomStatusForTask(Task t) {
    return this.taskToGroomMap.get(t);
  }

  public synchronized Task obtainNewTask(
      Map<String, GroomServerStatus> groomStatuses) {
    this.clusterSize = groomStatuses.size();

    if (this.status.getRunState() != JobStatus.RUNNING) {
      LOG.info("Cannot create task split for " + profile.getJobID());
      return null;
    }

    Task result = null;

    try {
      for (int i = 0; i < tasks.length; i++) {
        if (!tasks[i].isRunning() && !tasks[i].isComplete()) {
          result = tasks[i].getTaskToRun(groomStatuses, tasksInGroomMap);
          if (result != null)
            this.taskToGroomMap.put(result, tasks[i].getGroomServerStatus());
          int taskInGroom = 0;
          if (tasksInGroomMap.containsKey(tasks[i].getGroomServerStatus())) {
            taskInGroom = tasksInGroomMap.get(tasks[i].getGroomServerStatus());
          }
          tasksInGroomMap.put(tasks[i].getGroomServerStatus(), taskInGroom + 1);
          break;
        }
      }

    } catch (IOException e) {
      LOG.error("Exception while obtaining new task!", e);
    }
    counters.incrCounter(JobCounter.LAUNCHED_TASKS, 1L);
    return result;
  }

  public synchronized void completedTask(TaskInProgress tip, TaskStatus status) {
    TaskAttemptID taskid = status.getTaskId();
    updateTaskStatus(tip, status);
    LOG.debug("Taskid '" + taskid + "' has finished successfully.");
    tip.completed(taskid);

    //
    // If all tasks are complete, then the job is done!
    //

    boolean allDone = true;
    for (TaskInProgress taskInProgress : tasks) {
      if (!taskInProgress.isComplete()) {
        allDone = false;
        break;
      }
    }

    if (allDone) {
      this.status = new JobStatus(this.status.getJobID(), this.profile
          .getUser(), superstepCounter, superstepCounter, superstepCounter,
          JobStatus.SUCCEEDED, superstepCounter, counters);
      this.finishTime = System.currentTimeMillis();
      this.status.setFinishTime(this.finishTime);

      LOG.info("Job successfully done.");

      // delete job root
      master.deleteJobRoot(this.getJobID().toString());

      garbageCollect();
    }
  }

  public void failedTask(TaskInProgress tip, TaskStatus status) {
    TaskAttemptID taskid = status.getTaskId();
    updateTaskStatus(tip, status);
    LOG.info("Taskid '" + taskid + "' has failed.");
    tip.terminated(taskid);
    tip.kill();

    boolean allDone = true;
    for (TaskInProgress taskInProgress : tasks) {
      if (taskInProgress.isFailed()) {
        allDone = false;
        break;
      }
    }

    // TODO

    if (!allDone) {
      // Kill job
      this.kill();
      // Send KillTaskAction to GroomServer
      this.status = new JobStatus(this.status.getJobID(), this.profile
          .getUser(), 0L, 0L, 0L, JobStatus.KILLED, superstepCounter, counters);
      this.finishTime = System.currentTimeMillis();
      this.status.setFinishTime(this.finishTime);

      LOG.info("Job failed.");

      garbageCollect();
    }
  }

  public synchronized void updateTaskStatus(TaskInProgress tip,
      TaskStatus taskStatus) {
    TaskAttemptID taskid = taskStatus.getTaskId();

    tip.updateStatus(taskStatus); // update tip

    TaskStatus.State state = taskStatus.getRunState();
    TaskCompletionEvent taskEvent = null;
    // FIXME port number should be configurable
    String httpTaskLogLocation = "http://"
        + tip.getGroomServerStatus().getGroomHostName() + ":"
        + conf.getInt("bsp.http.groomserver.port", Constants.DEFAULT_GROOM_INFO_SERVER);

    if (state == TaskStatus.State.FAILED || state == TaskStatus.State.KILLED) {
      int eventNumber;
      if ((eventNumber = tip.getSuccessEventNumber()) != -1) {
        TaskCompletionEvent t = this.taskCompletionEvents.get(eventNumber);
        if (t.getTaskAttemptId().equals(taskid))
          t.setTaskStatus(TaskCompletionEvent.Status.OBSOLETE);
      }

      // Did the task failure lead to tip failure?
      TaskCompletionEvent.Status taskCompletionStatus = (state == TaskStatus.State.FAILED) ? TaskCompletionEvent.Status.FAILED
          : TaskCompletionEvent.Status.KILLED;
      if (tip.isFailed()) {
        taskCompletionStatus = TaskCompletionEvent.Status.TIPFAILED;
      }
      taskEvent = new TaskCompletionEvent(taskCompletionEventTracker, taskid,
          tip.idWithinJob(), taskCompletionStatus, httpTaskLogLocation);

      if (taskEvent != null) {
        this.taskCompletionEvents.add(taskEvent);
        taskCompletionEventTracker++;
      }
    }

    if (superstepCounter < taskStatus.getSuperstepCount()) {
      superstepCounter = taskStatus.getSuperstepCount();
      // TODO Later, we have to update JobInProgress status here

    }
  }

  public synchronized void kill() {
    if (status.getRunState() != JobStatus.KILLED) {
      this.status = new JobStatus(status.getJobID(), this.profile.getUser(),
          0L, 0L, 0L, JobStatus.KILLED, counters);
      this.finishTime = System.currentTimeMillis();
      this.status.setFinishTime(this.finishTime);
      //
      // kill all TIPs.
      //
      for (int i = 0; i < tasks.length; i++) {
        tasks[i].kill();
      }

      garbageCollect();
    }

  }

  /**
   * The job is dead. We're now GC'ing it, getting rid of the job from all
   * tables. Be sure to remove all of this job's tasks from the various tables.
   */
  synchronized void garbageCollect() {
    try {
      // Definitely remove the local-disk copy of the job file
      if (localJobFile != null) {
        localFs.delete(localJobFile, true);
        localJobFile = null;
      }
      if (localJarFile != null) {
        localFs.delete(localJarFile, true);
        localJarFile = null;
      }

      // JobClient always creates a new directory with job files
      // so we remove that directory to cleanup
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path(profile.getJobFile()).getParent(), true);

    } catch (IOException e) {
      LOG.info("Error cleaning up " + profile.getJobID() + ": " + e);
    }
  }

  /**
   * Get the number of times the job has restarted
   */
  int getNumRestarts() {
    return restartCount;
  }

  /**
   * @param jobName the jobName to set
   */
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  /**
   * @return the jobName
   */
  public String getJobName() {
    return jobName;
  }

  public Counters getCounters() {
    return counters;
  }

  List<TaskCompletionEvent> taskCompletionEvents;

  synchronized int getNumTaskCompletionEvents() {
    return taskCompletionEvents.size();
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(int fromEventId,
      int maxEvents) {
    TaskCompletionEvent[] events = TaskCompletionEvent.EMPTY_ARRAY;
    if (taskCompletionEvents.size() > fromEventId) {
      int actualMax = Math.min(maxEvents,
          (taskCompletionEvents.size() - fromEventId));
      events = taskCompletionEvents.subList(fromEventId,
          actualMax + fromEventId).toArray(events);
    }
    return events;
  }

}
