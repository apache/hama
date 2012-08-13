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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hama.bsp.BSPJobClient.RawSplit;

/**
 * TaskInProgress maintains all the info needed for a Task in the lifetime of
 * its owning Job.
 */
public class TaskInProgress {
  public static final Log LOG = LogFactory.getLog(TaskInProgress.class);

  private Configuration conf;

  // Constants
  static final int MAX_TASK_EXECS = 1;
  int maxTaskAttempts = 4;
  private AtomicBoolean failed = new AtomicBoolean(false);
  private static final int NUM_ATTEMPTS_PER_RESTART = 1000;

  // Job Meta
  private String jobFile = null;
  private int partition;
  private TaskID id;
  private JobInProgress job;
  private int completes = 0;

  private GroomServerStatus myGroomStatus = null;

  // Status
  // private double progress = 0;
  // private String state = "";
  private long startTime = 0;
  private int successEventNumber = -1;

  // The 'next' usable taskid of this tip
  int nextTaskId = 0;

  // The taskid that took this TIP to SUCCESS
  private TaskAttemptID successfulTaskId;

  // The first taskid of this tip
  private TaskAttemptID firstTaskId;
  
  private TaskAttemptID currentTaskId;

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

  private RawSplit rawSplit;

  private int mySuperstep = -1;

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

    init(jobId);
  }

  /**
   * 
   * @param jobId
   * @param jobFile
   * @param rawSplit
   * @param conf
   * @param job
   * @param partition
   */
  public TaskInProgress(BSPJobID jobId, String jobFile, RawSplit rawSplit,
      Configuration conf, JobInProgress job, int partition) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.rawSplit = rawSplit;
    this.job = job;
    this.setConf(conf);
    this.partition = partition;

    init(jobId);
  }

  /**
   * 
   * @param jobId
   */
  private void init(BSPJobID jobId) {
    this.id = new TaskID(jobId, partition);
    this.startTime = System.currentTimeMillis();
  }

  /**
   * 
   * @param taskid
   * @param grooms
   * @param tasksInGroomMap
   * @param possibleLocations
   * @return
   */
  private String getGroomToSchedule(TaskAttemptID taskid,
      Map<String, GroomServerStatus> grooms,
      Map<GroomServerStatus, Integer> tasksInGroomMap,
      String[] possibleLocations) {

    for (int i = 0; i < possibleLocations.length; ++i) {
      String location = possibleLocations[i];
      GroomServerStatus groom = grooms.get(location);
      if (groom == null)
        continue;
      Integer taskInGroom = tasksInGroomMap.get(groom);
      taskInGroom = (taskInGroom == null) ? 0 : taskInGroom;
      if (taskInGroom < groom.getMaxTasks()
          && location.equals(groom.getGroomHostName())) {
        return groom.getGroomHostName();
      }
    }
    return null;
  }

  /**
   * 
   * @param grooms
   * @param tasksInGroomMap
   * @return
   */
  private String getAnyGroomToSchedule(Map<String, GroomServerStatus> grooms,
      Map<GroomServerStatus, Integer> tasksInGroomMap) {

    Iterator<String> groomIter = grooms.keySet().iterator();
    while (groomIter.hasNext()) {
      GroomServerStatus groom = grooms.get(groomIter.next());
      if (groom == null)
        continue;
      Integer taskInGroom = tasksInGroomMap.get(groom);
      taskInGroom = (taskInGroom == null) ? 0 : taskInGroom;
      if (taskInGroom < groom.getMaxTasks()) {
        return groom.getGroomHostName();
      }
    }
    return null;
  }

  /**
   * 
   * @param groomStatus
   * @param grooms
   * @return
   */
  public Task constructTask(GroomServerStatus groomStatus) {
    if(groomStatus == null){
      return null;
    }
    TaskAttemptID taskId = computeTaskId();
    if (taskId == null) {
      return null;
    } else {
      String splitClass = null;
      BytesWritable split = null;
      if (rawSplit != null) {
    	  splitClass = rawSplit.getClassName();
    	  split = rawSplit.getBytes();
      }
      currentTaskId = taskId;
      String groomName = groomStatus.getGroomHostName();
      Task t = new BSPTask(jobId, jobFile, taskId, partition, splitClass, split);
      activeTasks.put(taskId, groomName);
      myGroomStatus = groomStatus;
      return t;
    }

  }

  private Task getGroomForRecoverTaskInHosts(TaskAttemptID taskid,
      Map<String, GroomServerStatus> grooms,
      Map<GroomServerStatus, Integer> tasksInGroomMap,
      String[] possibleLocations) {
    String splitClass = null;
    BytesWritable split = null;
    if (rawSplit != null) {
  	  splitClass = rawSplit.getClassName();
  	  split = rawSplit.getBytes();
    }
    Task t = null;
    String groomName = getGroomToSchedule(taskid, grooms, tasksInGroomMap,
        possibleLocations);
    if (groomName != null) {
      t = new BSPTask(jobId, jobFile, taskid, partition, splitClass, split);
      activeTasks.put(taskid, groomName);
      myGroomStatus = grooms.get(groomName);
    }

    if (t == null) {
      groomName = getAnyGroomToSchedule(grooms, tasksInGroomMap);
      if (groomName != null) {
        t = new BSPTask(jobId, jobFile, taskid, partition, splitClass, split);
        activeTasks.put(taskid, groomName);
        myGroomStatus = grooms.get(groomName);
      }
    }

    return t;
  }

  public Task getRecoveryTask(Map<String, GroomServerStatus> grooms,
      Map<GroomServerStatus, Integer> tasksInGroomMap, String[] hostNames)
      throws IOException {
    Integer count = tasksInGroomMap.get(myGroomStatus);
    if (count != null) {
      tasksInGroomMap.put(myGroomStatus, count - 1);
    }

    TaskAttemptID taskId = computeTaskId();
    LOG.debug("Recovering task = " + String.valueOf(taskId));
    if (taskId == null) {
      return null;
    } else {
      return getGroomForRecoverTaskInHosts(taskId, grooms, tasksInGroomMap,
          hostNames);
    }
  }

  /**
   * 
   * @return
   */
  public boolean canStartTask() {
    return (nextTaskId < (MAX_TASK_EXECS + maxTaskAttempts));
  }

  private TaskAttemptID computeTaskId() {
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
    return taskid;
  }

  // /** Remove */
  // public Task getTaskToRun(Map<String, GroomServerStatus> grooms,
  // Map<GroomServerStatus, Integer> tasksInGroomMap) throws IOException {
  // TaskAttemptID taskId = computeTaskId();
  // if (taskId == null) {
  // return null;
  // } else {
  // return getGroomForTask(taskId, grooms, tasksInGroomMap);
  // }
  // }

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

  public GroomServerStatus getGroomServerStatus() {
    return myGroomStatus;
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
    TaskStatus ts = taskStatuses.get(taskid);
    if ((ts != null) && (!tasksReportedClosed.contains(taskid))
        && (job.getStatus().getRunState() != JobStatus.RUNNING)) {
      tasksReportedClosed.add(taskid);
      return true;
    } else {
      return false;
    }
  }

  public void completed(TaskAttemptID taskid) {
    LOG.debug("Task '" + taskid.getTaskID().toString() + "' has completed.");

    TaskStatus status = taskStatuses.get(taskid);
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

  public void terminated(TaskAttemptID taskid) {
    LOG.info("Task '" + taskid.getTaskID().toString() + "' has failed.");

    TaskStatus status = taskStatuses.get(taskid);
    status.setRunState(TaskStatus.State.FAILED);
    activeTasks.remove(taskid);
  }

  private void setSuccessfulTaskid(TaskAttemptID taskid) {
    this.successfulTaskId = taskid;
  }

  private TaskAttemptID getSuccessfulTaskid() {
    return successfulTaskId;
  }

  public boolean updateStatus(TaskStatus status) {
    TaskAttemptID taskid = status.getTaskId();
    TaskStatus oldStatus = taskStatuses.get(taskid);
    boolean changed = true;

    if (oldStatus != null) {
      TaskStatus.State oldState = oldStatus.getRunState();
      TaskStatus.State newState = status.getRunState();
      changed = oldState != newState;
    }

    taskStatuses.put(taskid, status);
    return changed;
  }

  public TaskStatus getTaskStatus(TaskAttemptID taskId) {
    return this.taskStatuses.get(taskId);
  }

  public void kill() {
    this.failed.set(true);
  }

  public boolean isFailed() {
    return failed.get();
  }

  /**
   * @param conf the conf to set
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return the conf
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Set the event number that was raised for this tip
   */
  public void setSuccessEventNumber(int eventNumber) {
    successEventNumber = eventNumber;
  }

  /**
   * Get the event number that was raised for this tip
   */
  public int getSuccessEventNumber() {
    return successEventNumber;
  }

  /**
   * @return int the tip index
   */
  public int idWithinJob() {
    return partition;
  }

  public String machineWhereTaskRan(TaskAttemptID taskid) {
    return taskStatuses.get(taskid).getGroomServer();
  }

  public int getSuperstep() {
    return mySuperstep;
  }

  public void setSuperstep(int mySuperstep) {
    this.mySuperstep = mySuperstep;
  }

  // TODO: In future this should be extended to the list of resources that the
  // task requires.
  public RawSplit getFileSplit() {
    return this.rawSplit;
  }
  
  public TaskAttemptID getCurrentTaskAttemptId(){
    return this.currentTaskId;
  }

}
