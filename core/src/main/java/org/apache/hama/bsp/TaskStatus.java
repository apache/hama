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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.bsp.BSPPeerImpl.PeerCounter;

/**
 * Describes the current status of a task. This is not intended to be a
 * comprehensive piece of data.
 */
public class TaskStatus implements Writable, Cloneable {
  static final Log LOG = LogFactory.getLog(TaskStatus.class);

  // enumeration for reporting current phase of a task.
  public static enum Phase {
    STARTING, COMPUTE, BARRIER_SYNC, CLEANUP
  }

  // what state is the task in?
  public static enum State {
    RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED, COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN
  }

  private BSPJobID jobId;
  private TaskAttemptID taskId;
  private float progress;
  private volatile State runState;
  private String stateString;
  private String groomServer;

  private long startTime;
  private long finishTime;

  private volatile Phase phase = Phase.STARTING;

  private Counters counters;
  
  /**
   * 
   */
  public TaskStatus() {
    jobId = new BSPJobID();
    taskId = new TaskAttemptID();
  }

  public TaskStatus(BSPJobID jobId, TaskAttemptID taskId, float progress,
      State runState, String stateString, String groomServer, Phase phase, Counters counters) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.progress = progress;
    this.runState = runState;
    this.stateString = stateString;
    this.groomServer = groomServer;
    this.phase = phase;
    this.counters = counters;
  }

  // //////////////////////////////////////////////////
  // Accessors and Modifiers
  // //////////////////////////////////////////////////

  public BSPJobID getJobId() {
    return jobId;
  }

  public TaskAttemptID getTaskId() {
    return taskId;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public State getRunState() {
    return runState;
  }

  public void setRunState(State state) {
    this.runState = state;
  }

  public String getStateString() {
    return stateString;
  }

  public void setStateString(String stateString) {
    this.stateString = stateString;
  }

  public String getGroomServer() {
    return groomServer;
  }

  public void setGroomServer(String groomServer) {
    this.groomServer = groomServer;
  }

  public long getFinishTime() {
    return finishTime;
  }

  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * Get start time of the task.
   * 
   * @return 0 is start time is not set, else returns start time.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set startTime of the task.
   * 
   * @param startTime start time
   */
  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get current phase of this task.
   * 
   * @return .
   */
  public Phase getPhase() {
    return this.phase;
  }

  /**
   * Set current phase of this task.
   * 
   * @param phase phase of this task
   */
  void setPhase(Phase phase) {
    this.phase = phase;
  }

  /**
   * Get task's counters.
   */
  public Counters getCounters() {
    return counters;
  }
  /**
   * Set the task's counters.
   * @param counters
   */
  public void setCounters(Counters counters) {
    this.counters = counters;
  }
  
  /**
   * Update the status of the task.
   * 
   * This update is done by ping thread before sending the status.
   * 
   * @param progress
   * @param state
   * @param counters
   */
  synchronized void statusUpdate(float progress, String state, Counters counters) {
    setProgress(progress);
    setStateString(state);
    setCounters(counters);
  }

  /**
   * Update the status of the task.
   * 
   * @param status updated status
   */
  synchronized void statusUpdate(TaskStatus status) {
    this.counters = status.getCounters();
    
    this.progress = status.getProgress();
    this.runState = status.getRunState();
    this.stateString = status.getStateString();

    if (status.getStartTime() != 0) {
      this.startTime = status.getStartTime();
    }
    if (status.getFinishTime() != 0) {
      this.finishTime = status.getFinishTime();
    }

    this.phase = status.getPhase();
  }

  /**
   * Update specific fields of task status
   * 
   * This update is done in BSPMaster when a cleanup attempt of task reports its
   * status. Then update only specific fields, not all.
   * 
   * @param runState
   * @param progress
   * @param state
   * @param phase
   * @param finishTime
   */
  synchronized void statusUpdate(State runState, float progress, String state,
      Phase phase, long finishTime) {
    setRunState(runState);
    setProgress(progress);
    setStateString(state);
    setPhase(phase);
    if (finishTime != 0) {
      this.finishTime = finishTime;
    }
  }

  /**
   * @return The number of BSP super steps executed by the task.
   */
  public long getSuperstepCount() {
    return counters.getCounter(PeerCounter.SUPERSTEP_SUM);
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }

  // ////////////////////////////////////////////
  // Writable
  // ////////////////////////////////////////////

  @Override
  public void readFields(DataInput in) throws IOException {
    this.jobId.readFields(in);
    this.taskId.readFields(in);
    this.progress = in.readFloat();
    this.runState = WritableUtils.readEnum(in, State.class);
    this.stateString = Text.readString(in);
    this.phase = WritableUtils.readEnum(in, Phase.class);
    this.startTime = in.readLong();
    this.finishTime = in.readLong();
    
    counters = new Counters();
    this.counters.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    jobId.write(out);
    taskId.write(out);
    out.writeFloat(progress);
    WritableUtils.writeEnum(out, runState);
    Text.writeString(out, stateString);
    WritableUtils.writeEnum(out, phase);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    
    counters.write(out);
  }
}
