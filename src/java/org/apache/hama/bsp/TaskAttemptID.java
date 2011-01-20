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

/**
 * TaskAttemptID is a unique identifier for a task attempt.
 */
public class TaskAttemptID extends ID {
  protected static final String ATTEMPT = "attempt";
  private TaskID taskId;

  public TaskAttemptID(TaskID taskId, int id) {
    super(id);
    if (taskId == null) {
      throw new IllegalArgumentException("taskId cannot be null");
    }
    this.taskId = taskId;
  }

  public TaskAttemptID(String jtIdentifier, int jobId, boolean isMatrixTask,
      int taskId, int id) {
    this(new TaskID(jtIdentifier, jobId, isMatrixTask, taskId), id);
  }

  public TaskAttemptID() {
    taskId = new TaskID();
  }

  public BSPJobID getJobID() {
    return taskId.getJobID();
  }
 
  public TaskID getTaskID() {
    return taskId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskAttemptID that = (TaskAttemptID) o;
    return this.taskId.equals(that.taskId);
  }

  protected StringBuilder appendTo(StringBuilder builder) {
    return taskId.appendTo(builder).append(SEPARATOR).append(id);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    taskId.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    taskId.write(out);
  }

  @Override
  public int hashCode() {
    return taskId.hashCode() * 5 + id;
  }

  @Override
  public int compareTo(ID o) {
    TaskAttemptID that = (TaskAttemptID) o;
    int tipComp = this.taskId.compareTo(that.taskId);
    if (tipComp == 0) {
      return this.id - that.id;
    } else
      return tipComp;
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(ATTEMPT)).toString();
  }

  public static TaskAttemptID forName(String str)
      throws IllegalArgumentException {
    if (str == null)
      return null;
    try {
      String[] parts = str.split(Character.toString(SEPARATOR));
      if (parts.length == 6) {
        if (parts[0].equals(ATTEMPT)) {
          boolean isMatrixTask = false;
          if (parts[3].equals("m"))
            isMatrixTask = true;
          else if (parts[3].equals("g"))
            isMatrixTask = false;
          else
            throw new Exception();

          return new TaskAttemptID(parts[1], Integer.parseInt(parts[2]),
              isMatrixTask, Integer.parseInt(parts[4]), Integer
                  .parseInt(parts[5]));
        }
      }
    } catch (Exception ex) {
      // fall below
    }
    throw new IllegalArgumentException("TaskAttemptId string : " + str
        + " is not properly formed");
  }
}
