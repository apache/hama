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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.bsp.ft.AsyncRcvdMsgCheckpointImpl;
import org.apache.hama.bsp.ft.BSPFaultTolerantService;
import org.apache.hama.bsp.ft.FaultTolerantMasterService;
import org.apache.hama.bsp.sync.MasterSyncClient;
import org.apache.hama.bsp.taskallocation.BSPResource;
import org.apache.hama.bsp.taskallocation.BestEffortDataLocalTaskAllocator;
import org.apache.hama.bsp.taskallocation.TaskAllocationStrategy;
import org.apache.hama.util.ReflectionUtils;

/**
 * JobInProgress maintains all the info for keeping a Job on the straight and
 * narrow. It keeps its JobProfile and its latest JobStatus, plus a set of
 * tables for doing bookkeeping of its Tasks.ss
 */
public class JobInProgress {

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
    LAUNCHED_TASKS, SUPERSTEPS
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

  int maxTaskAttempts;

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
  Map<GroomServerStatus, Integer> taskCountInGroomMap;

  // If the task does not exist as key, it implies that the task did not fail
  // before.
  // Value in the map implies the attempt ID for which the key(task) was
  // re-attempted before.
  Map<Task, Integer> taskReattemptMap;

  Set<TaskInProgress> recoveryTasks;

  // This set keeps track of the tasks that have failed.
  Set<Task> failedTasksTillNow;

  private int taskCompletionEventTracker = 0;

  private TaskAllocationStrategy taskAllocationStrategy;

  private FaultTolerantMasterService faultToleranceService;
  
  /**
   * Used only for unit tests.
   * @param jobId
   * @param conf
   */
  public JobInProgress(BSPJobID jobId, Configuration conf){
    this.conf = conf;
    this.jobId = jobId;
    master = null;
  }

  public JobInProgress(BSPJobID jobId, Path jobFile, BSPMaster master,
      Configuration conf) throws IOException {
    this.conf = conf;
    this.jobId = jobId;
    this.localFs = FileSystem.getLocal(conf);
    this.jobFile = jobFile;
    this.master = master;

    this.status = new JobStatus(jobId, null, 0L, 0L,
        JobStatus.State.PREP.value(), counters);
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
    this.jobSplit = job.getConfiguration().get("bsp.job.split.file");

    this.numBSPTasks = job.getNumBspTask();
    this.taskCompletionEvents = new ArrayList<TaskCompletionEvent>(
        numBSPTasks + 10);

    this.maxTaskAttempts = job.getConfiguration().getInt(Constants.MAX_TASK_ATTEMPTS,
        Constants.DEFAULT_MAX_TASK_ATTEMPTS);

    this.profile = new JobProfile(job.getUser(), jobId, jobFile.toString(),
        job.getJobName());

    this.setJobName(job.getJobName());

    status.setUsername(job.getUser());
    status.setStartTime(startTime);

    String jarFile = job.getJar();
    if (jarFile != null) {
      fs.copyToLocalFile(new Path(jarFile), localJarFile);
    }

    failedTasksTillNow = new HashSet<Task>(2 * tasks.length);

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

  @Override
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
            splits[i], this.conf, this, i);
      }
    } else {
      this.tasks = new TaskInProgress[numBSPTasks];
      for (int i = 0; i < numBSPTasks; i++) {
        tasks[i] = new TaskInProgress(getJobID(), this.jobFile.toString(),
            null, this.conf, this, i);
      }
    }
    this.taskToGroomMap = new HashMap<Task, GroomServerStatus>(2 * tasks.length);

    this.taskCountInGroomMap = new HashMap<GroomServerStatus, Integer>();

    this.recoveryTasks = new HashSet<TaskInProgress>(2 * tasks.length);

    // Update job status
    this.status = new JobStatus(this.status.getJobID(), this.profile.getUser(),
        0L, 0L, JobStatus.RUNNING, counters);

    // delete all nodes belonging to that job before start
    MasterSyncClient syncClient = master.getSyncClient();
    syncClient.registerJob(this.getJobID().toString());

    tasksInited = true;

    Class<?> taskAllocatorClass = conf.getClass(Constants.TASK_ALLOCATOR_CLASS,
        BestEffortDataLocalTaskAllocator.class, TaskAllocationStrategy.class);
    this.taskAllocationStrategy = (TaskAllocationStrategy) ReflectionUtils
        .newInstance(taskAllocatorClass, new Object[0]);

    if (conf.getBoolean(Constants.FAULT_TOLERANCE_FLAG, false)) {

      Class<?> ftClass = conf.getClass(Constants.FAULT_TOLERANCE_CLASS, 
          AsyncRcvdMsgCheckpointImpl.class ,
          BSPFaultTolerantService.class);
      if (ftClass != null) {
        try {
          faultToleranceService = ((BSPFaultTolerantService<?>) ReflectionUtils
              .newInstance(ftClass, new Object[0]))
              .constructMasterFaultTolerance(jobId, maxTaskAttempts, tasks,
                  conf, master.getSyncClient(), taskAllocationStrategy);
          LOG.info("Initialized fault tolerance service with "
              + ftClass.getCanonicalName());
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }

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
    BSPResource[] resources = new BSPResource[0];

    for (int i = 0; i < tasks.length; i++) {
      if (!tasks[i].isRunning() && !tasks[i].isComplete()) {

        String[] selectedGrooms = taskAllocationStrategy.selectGrooms(
            groomStatuses, taskCountInGroomMap, resources, tasks[i]);
        GroomServerStatus groomStatus = taskAllocationStrategy
            .getGroomToAllocate(groomStatuses, selectedGrooms,
                taskCountInGroomMap, resources, tasks[i]);
        if (groomStatus != null){
          result = tasks[i].constructTask(groomStatus);
        }
        else if (LOG.isDebugEnabled()){
        	LOG.debug("Could not find a groom to schedule task");
        }
        if (result != null) {
          updateGroomTaskDetails(tasks[i].getGroomServerStatus(), result);
        }
        break;
      }
    }

    counters.incrCounter(JobCounter.LAUNCHED_TASKS, 1L);
    return result;
  }

  public void recoverTasks(Map<String, GroomServerStatus> groomStatuses,
      Map<GroomServerStatus, List<GroomServerAction>> actionMap)
      throws IOException {

    if (this.faultToleranceService == null)
      return;

    try {
      this.faultToleranceService.recoverTasks(this, groomStatuses,
          fetchAndClearTasksToRecover(), tasks, taskCountInGroomMap, actionMap);
    } catch (IOException e) {
      throw e;
    }
  }

  private void updateGroomTaskDetails(GroomServerStatus groomStatus, Task task) {
    taskToGroomMap.put(task, groomStatus);
    int tasksInGroom = 0;

    if (taskCountInGroomMap.containsKey(groomStatus)) {
      tasksInGroom = taskCountInGroomMap.get(groomStatus);
    }
    taskCountInGroomMap.put(groomStatus, tasksInGroom + 1);
  }

  /**
   * Hosts that tasks run on.
   * 
   * @return groom host name that tasks of a job run on.
   */
  public synchronized String[] tasksOnGroomServers() {
    final String[] list = new String[tasks.length];
    for (int i = 0; i < tasks.length; i++) {
      list[i] = tasks[i].getGroomServerStatus().getGroomHostName();
    }
    return list;
  }

  /**
   * Mark the completed task status. If all the tasks are completed the status
   * of the job is updated to notify the client on the completion of the whole
   * job.
   * 
   * @param tip <code>TaskInProgress</code> object representing task.
   * @param status The completed task status
   */
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
      this.status = new JobStatus(this.status.getJobID(),
          this.profile.getUser(), superstepCounter, superstepCounter,
          superstepCounter, JobStatus.SUCCEEDED, superstepCounter, counters);
      this.finishTime = System.currentTimeMillis();
      this.status.setFinishTime(this.finishTime);

      LOG.info("Job successfully done.");

      // delete job root
      master.getSyncClient().deregisterJob(this.getJobID().toString());

      garbageCollect();
    }
  }

  /**
   * Mark failure of a task.
   * 
   * @param tip <code>TaskInProgress</code> object representing task.
   * @param status The failed task status
   */
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

    if (!allDone) {
      // Kill job
      this.kill();
      // Send KillTaskAction to GroomServer
      this.status = new JobStatus(this.status.getJobID(),
          this.profile.getUser(), 0L, 0L, 0L, JobStatus.KILLED,
          superstepCounter, counters);
      this.finishTime = System.currentTimeMillis();
      this.status.setFinishTime(this.finishTime);

      LOG.info("Job failed.");

      garbageCollect();
    }
  }

  /**
   * Updates the task status of the task.
   * 
   * @param tip <code>TaskInProgress</code> representing task
   * @param taskStatus The status of the task.
   */
  public synchronized void updateTaskStatus(TaskInProgress tip,
      TaskStatus taskStatus) {
    TaskAttemptID taskid = taskStatus.getTaskId();
    boolean change = tip.updateStatus(taskStatus); // update tip

    if (change) {
      TaskStatus.State state = taskStatus.getRunState();
      TaskCompletionEvent taskEvent = null;
      String httpTaskLogLocation = "http://"
          + tip.getGroomServerStatus().getGroomHostName()
          + ":"
          + conf.getInt("bsp.http.groomserver.port",
              Constants.DEFAULT_GROOM_INFO_SERVER);

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
    }

    if (superstepCounter < taskStatus.getSuperstepCount()) {
      superstepCounter = taskStatus.getSuperstepCount();
    }
  }

  /**
   * Kill the job.
   */
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
      
      if(LOG.isDebugEnabled()){
        LOG.debug("Removing " + localJobFile + " and " + localJarFile
            + " getJobFile = " + profile.getJobFile());
      }
      
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

  /**
   * Returns the configured maximum number of times the task could be
   * re-attempted.
   */
  int getMaximumReAttempts() {
    return maxTaskAttempts;
  }

  /**
   * Returns true if the task should be restarted on failure. It also causes
   * JobInProgress object to maintain state of the restart request.
   */
  synchronized boolean handleFailure(TaskInProgress tip) {
    if (this.faultToleranceService == null
        || (!faultToleranceService.isRecoveryPossible(tip)))
      return false;

    if (!faultToleranceService.isAlreadyRecovered(tip)) {
      if(LOG.isDebugEnabled()){
        LOG.debug("Adding recovery task " + tip.getCurrentTaskAttemptId());
      }
      recoveryTasks.add(tip);
      status.setRunState(JobStatus.RECOVERING);
      return true;
    }
    else if(LOG.isDebugEnabled()){
      LOG.debug("Avoiding recovery task " + tip.getCurrentTaskAttemptId());
    }
    return false;
    
  }
  
  
  /**
   * 
   * @return Returns the list of tasks in progress that has to be recovered.
   */
  synchronized TaskInProgress[] fetchAndClearTasksToRecover() {
    TaskInProgress[] failedTasksInProgress = new TaskInProgress[recoveryTasks
        .size()];
    recoveryTasks.toArray(failedTasksInProgress);

    recoveryTasks.clear();
    return failedTasksInProgress;
  }

  public boolean isRecoveryPending() {
    return recoveryTasks.size() != 0;
  }

  public Set<Task> getTaskSet() {
    return taskToGroomMap.keySet();
  }

  public FaultTolerantMasterService getFaultToleranceService() {
    return this.faultToleranceService;
  }

}
