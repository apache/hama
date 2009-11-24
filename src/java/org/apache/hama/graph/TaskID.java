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

package org.apache.hama.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

public class TaskID extends ID {
  protected static final String TASK = "task";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }

  private JobID jobId;
  private boolean isMatrixTask;

  public TaskID(JobID jobId, boolean isMatrixTask, int id) {
    super(id);
    if (jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
    this.isMatrixTask = isMatrixTask;
  }

  public TaskID(String jtIdentifier, int jobId, boolean isGraphTask, int id) {
    this(new JobID(jtIdentifier, jobId), isGraphTask, id);
  }

  public TaskID() {
    jobId = new JobID();
  }

  /** Returns the {@link JobID} object that this tip belongs to */
  public JobID getJobID() {
    return jobId;
  }

  public boolean isGraphTask() {
    return isMatrixTask;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskID that = (TaskID) o;
    return this.isMatrixTask == that.isMatrixTask
        && this.jobId.equals(that.jobId);
  }

  @Override
  public int compareTo(ID o) {
    TaskID that = (TaskID) o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if (jobComp == 0) {
      if (this.isMatrixTask == that.isMatrixTask) {
        return this.id - that.id;
      } else
        return this.isMatrixTask ? -1 : 1;
    } else {
      return jobComp;
    }
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(TASK)).toString();
  }

  protected StringBuilder appendTo(StringBuilder builder) {
    return jobId.appendTo(builder).append(SEPARATOR).append(
        isMatrixTask ? 'm' : 'g').append(SEPARATOR).append(idFormat.format(id));
  }

  @Override
  public int hashCode() {
    return jobId.hashCode() * 524287 + id;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    jobId.readFields(in);
    isMatrixTask = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
    out.writeBoolean(isMatrixTask);
  }

  public static TaskID forName(String str) throws IllegalArgumentException {
    if (str == null)
      return null;
    try {
      String[] parts = str.split("_");
      if (parts.length == 5) {
        if (parts[0].equals(TASK)) {
          boolean isMatrixTask = false;
          if (parts[3].equals("m"))
            isMatrixTask = true;
          else if (parts[3].equals("g"))
            isMatrixTask = false;
          else
            throw new Exception();
          return new TaskID(parts[1], Integer.parseInt(parts[2]), isMatrixTask,
              Integer.parseInt(parts[4]));
        }
      }
    } catch (Exception ex) {
    }
    throw new IllegalArgumentException("TaskId string : " + str
        + " is not properly formed");
  }
}
