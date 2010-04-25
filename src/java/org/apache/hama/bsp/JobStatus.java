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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

public class JobStatus implements Writable, Cloneable {

  static {
    WritableFactories.setFactory(JobStatus.class, new WritableFactory() {
      public Writable newInstance() {
        return new JobStatus();
      }
    });
  }

  public static final int RUNNING = 1;
  public static final int SUCCEEDED = 2;
  public static final int FAILED = 3;
  public static final int PREP = 4;
  public static final int KILLED = 5;

  private JobID jobid;
  private float progress;
  private float cleanupProgress;
  private float setupProgress;
  private int runState;
  private long startTime;
  private String schedulingInfo = "NA";

  public JobStatus() {
  }

  public JobStatus(JobID jobid, float progress, int runState) {
    this(jobid, progress, 0.0f, runState);
  }

  public JobStatus(JobID jobid, float progress, float cleanupProgress,
      int runState) {
    this(jobid, 0.0f, progress, cleanupProgress, runState);
  }

  public JobStatus(JobID jobid, float setupProgress, float progress,
      float cleanupProgress, int runState) {
    this.jobid = jobid;
    this.setupProgress = setupProgress;
    this.progress = progress;
    this.cleanupProgress = cleanupProgress;
    this.runState = runState;
  }

  public JobID getJobID() {
    return jobid;
  }

  public synchronized float progress() {
    return progress;
  }

  synchronized void setprogress(float p) {
    this.progress = (float) Math.min(1.0, Math.max(0.0, p));
  }

  public synchronized float cleanupProgress() {
    return cleanupProgress;
  }

  synchronized void setCleanupProgress(float p) {
    this.cleanupProgress = (float) Math.min(1.0, Math.max(0.0, p));
  }

  public synchronized float setupProgress() {
    return setupProgress;
  }

  synchronized void setSetupProgress(float p) {
    this.setupProgress = (float) Math.min(1.0, Math.max(0.0, p));
  }

  public synchronized int getRunState() {
    return runState;
  }

  public synchronized void setRunState(int state) {
    this.runState = state;
  }

  synchronized void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  synchronized public long getStartTime() {
    return startTime;
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
    out.writeFloat(setupProgress);
    out.writeFloat(progress);
    out.writeFloat(cleanupProgress);
    out.writeInt(runState);
    out.writeLong(startTime);
    Text.writeString(out, schedulingInfo);
  }

  public synchronized void readFields(DataInput in) throws IOException {
    this.jobid = new JobID();
    jobid.readFields(in);
    this.setupProgress = in.readFloat();
    this.progress = in.readFloat();
    this.cleanupProgress = in.readFloat();
    this.runState = in.readInt();
    this.startTime = in.readLong();
    this.schedulingInfo = Text.readString(in);
  }
}
