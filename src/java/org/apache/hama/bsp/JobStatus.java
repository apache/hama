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
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Describes the current status of a job.
 */
public class JobStatus implements Writable, Cloneable {
  public static final Log LOG = LogFactory.getLog(JobStatus.class);
  
  static {
    WritableFactories.setFactory(JobStatus.class, new WritableFactory() {
      public Writable newInstance() {
        return new JobStatus();
      }
    });
  }

  public static enum State{
    RUNNING(1),
    SUCCEEDED(2),
    FAILED(3),
    PREP(4),
    KILLED(5);
    int s;
    State(int s){
      this.s = s;
    }
    public int value(){
      return this.s;
    }
  }

  public static final int RUNNING = 1;
  public static final int SUCCEEDED = 2;
  public static final int FAILED = 3;
  public static final int PREP = 4;
  public static final int KILLED = 5;

  private BSPJobID jobid;
  private long progress;
  private long cleanupProgress;
  private long setupProgress;
  private volatile State state;// runState in enum
  private int runState;
  private long startTime;
  private String schedulingInfo = "NA";
  private String user;
  private long superstepCount;

  private long finishTime;
  
  public JobStatus() {
  }

  public JobStatus(BSPJobID jobid, String user, long progress, int runState) {
    this(jobid, user, progress, 0, runState);
  }

  public JobStatus(BSPJobID jobid, String user, long progress, long cleanupProgress,
      int runState) {
    this(jobid, user, 0, progress, cleanupProgress, runState);
  }

  public JobStatus(BSPJobID jobid, String user, long setupProgress, long progress,
      long cleanupProgress, int runState) {
    this(jobid, user, 0, progress, cleanupProgress, runState, 0);
  }

  public JobStatus(BSPJobID jobid, String user, long setupProgress, long progress,
      long cleanupProgress, int runState, long superstepCount) {
    this.jobid = jobid;
    this.setupProgress = setupProgress;
    this.progress = progress;
    this.cleanupProgress = cleanupProgress;
    this.runState = runState;
    this.state = State.values()[runState-1];
    this.superstepCount = superstepCount;
    this.user = user;
  }
  
  public BSPJobID getJobID() {
    return jobid;
  }

  public synchronized long progress() {
    return progress;
  }

  synchronized void setprogress(long p) {
    this.progress = p;
  }

  public synchronized long cleanupProgress() {
    return cleanupProgress;
  }

  synchronized void setCleanupProgress(int p) {
    this.cleanupProgress = p;
  }

  public synchronized long setupProgress() {
    return setupProgress;
  }

  synchronized void setSetupProgress(long p) {
    this.setupProgress = p;
  }

  public JobStatus.State getState(){
    return this.state;
  }

  public void setState(JobStatus.State state){
    this.state = state;
  }

  public synchronized int getRunState() {
    return runState;
  }

  public synchronized void setRunState(int state) {
    this.runState = state;
  }

  public synchronized long getSuperstepCount() {
    return superstepCount;
  }
  
  public synchronized void setSuperstepCount(long superstepCount) {
    this.superstepCount = superstepCount;
  }
  
  public synchronized void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public synchronized long getStartTime() {
    return startTime;
  }

  public synchronized void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }
  
  /**
   * Get the finish time of the job.
   */
  public synchronized long getFinishTime() { 
    return finishTime;
  }
  
  /**
   * @param user The username of the job
   */
  public synchronized void setUsername(String user) {
    this.user = user;
  }

  /**
   * @return the username of the job
   */
  public synchronized String getUsername() {
    return user;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new InternalError(cnse.toString());
    }
  }

  public synchronized String getSchedulingInfo() {
    return schedulingInfo;
  }

  public synchronized void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  public synchronized boolean isJobComplete() {
    return (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED || runState == JobStatus.KILLED);
  }

  public synchronized void write(DataOutput out) throws IOException {
    jobid.write(out);
    out.writeLong(setupProgress);
    out.writeLong(progress);
    out.writeLong(cleanupProgress);
    out.writeInt(runState);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    Text.writeString(out, user);
    Text.writeString(out, schedulingInfo);
    out.writeLong(superstepCount);
  }

  public synchronized void readFields(DataInput in) throws IOException {
    this.jobid = new BSPJobID();
    jobid.readFields(in);
    this.setupProgress = in.readLong();
    this.progress = in.readLong();
    this.cleanupProgress = in.readLong();
    this.runState = in.readInt();
    this.startTime = in.readLong();
    this.finishTime = in.readLong();
    this.user = Text.readString(in);
    this.schedulingInfo = Text.readString(in);
    this.superstepCount = in.readLong();
  }

}
