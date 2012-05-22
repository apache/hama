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
 *TaskInProgress maintains all the info needed for a Task in the lifetime of
 * its owning Job.
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
  
  private GroomServerStatus myGroomStatus = null;

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

  private RawSplit rawSplit;
  
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

  public TaskInProgress(BSPJobID jobId, String jobFile, RawSplit rawSplit, BSPMaster master,
      Configuration conf, JobInProgress job, int partition) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.rawSplit = rawSplit;
    this.setBspMaster(master);
    this.job = job;
    this.setConf(conf);
    this.partition = partition;

    init(jobId);
  }

  private void init(BSPJobID jobId) {
    this.id = new TaskID(jobId, partition);
    this.startTime = System.currentTimeMillis();
  }
  
  /**
   * Return a Task that can be sent to a GroomServer for execution.
   */
  public Task getTaskToRun(Map<String, GroomServerStatus> grooms, 
      Map<GroomServerStatus, Integer> tasksInGroomMap) throws IOException {
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
    
    String splitClass = null;
    BytesWritable split = null;
    GroomServerStatus selectedGroom = null;
    if(rawSplit != null){
      splitClass = rawSplit.getClassName();
      split = rawSplit.getBytes();
      String[] possibleLocations = rawSplit.getLocations();
      for (int i = 0; i < possibleLocations.length; ++i){
        String location = possibleLocations[i];
        GroomServerStatus groom = grooms.get(location);
        Integer taskInGroom = tasksInGroomMap.get(groom);
        taskInGroom = (taskInGroom == null)?0:taskInGroom;
        if(taskInGroom < groom.getMaxTasks() && 
            location.equals(groom.getGroomHostName())){
            selectedGroom = groom;
            t = new BSPTask(jobId, jobFile, taskid, partition, splitClass, split);
            activeTasks.put(taskid, groom.getGroomName());
            
            break;
        }
      }
    }
    //Failed in attempt to get data locality or there was no input split.
    if(selectedGroom == null){
      Iterator<String> groomIter = grooms.keySet().iterator();
      while(groomIter.hasNext()) {
        GroomServerStatus groom = grooms.get(groomIter.next());
        Integer taskInGroom = tasksInGroomMap.get(groom);
        taskInGroom = (taskInGroom == null)?0:taskInGroom;
        if(taskInGroom < groom.getMaxTasks()){
          selectedGroom = groom;
          t = new BSPTask(jobId, jobFile, taskid, partition, splitClass, split);
          activeTasks.put(taskid, groom.getGroomName());
        }
      }
    }
    
    myGroomStatus = selectedGroom;

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
  
  public GroomServerStatus getGroomServerStatus(){
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

  public void updateStatus(TaskStatus status) {
    taskStatuses.put(status.getTaskId(), status);
  }

  public TaskStatus getTaskStatus(TaskAttemptID taskId) {
    return this.taskStatuses.get(taskId);
  }

  public void kill() {
    this.failed = true;
  }

  public boolean isFailed() {
    return failed;
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
   * @param bspMaster the bspMaster to set
   */
  public void setBspMaster(BSPMaster bspMaster) {
    this.bspMaster = bspMaster;
  }

  /**
   * @return the bspMaster
   */
  public BSPMaster getBspMaster() {
    return bspMaster;
  }

}
