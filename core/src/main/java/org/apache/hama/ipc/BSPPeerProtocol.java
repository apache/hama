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
package org.apache.hama.ipc;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hama.Constants;
import org.apache.hama.bsp.Task;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskStatus;

/**
 * Protocol that task child process uses to contact its parent process.
 */
public interface BSPPeerProtocol extends HamaRPCProtocolVersion, Closeable,
    Constants {

  /** Called when a child task process starts, to get its task. */
  Task getTask(TaskAttemptID taskid) throws IOException;

  /**
   * Periodically called by child to check if parent is still alive.
   * 
   * @return True if the task is known
   */
  boolean ping(TaskAttemptID taskid) throws IOException;

  /**
   * Report that the task is successfully completed. Failure is assumed if the
   * task process exits without calling this.
   * 
   * @param taskid task's id
   */
  void done(TaskAttemptID taskid) throws IOException;

  /** Report that the task encounted a local filesystem error. */
  void fsError(TaskAttemptID taskId, String message) throws IOException;

  /** Report that the task encounted a fatal error. */
  void fatalError(TaskAttemptID taskId, String message) throws IOException;
  
  /**
   * Report child's progress to parent.
   * 
   * @param taskId task-id of the child
   * @param taskStatus status of the child
   * @throws IOException
   * @throws InterruptedException
   * @return True if the task is known
   */
  boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException;

  /**
   * @param taskid
   * @return assigned port number
   */
  int getAssignedPortNum(TaskAttemptID taskid);

}
