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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class SimpleTaskScheduler extends TaskScheduler {
  private static final Log LOG = LogFactory.getLog(SimpleTaskScheduler.class);
  List<JobInProgress> jobQueue;

  public SimpleTaskScheduler() {
    jobQueue = new ArrayList<JobInProgress>();
  }

  @Override
  public void addJob(JobInProgress job) {
    LOG.debug(">> Added a job (" + job + ") to scheduler (remaining jobs: "
        + (jobQueue.size() + 1) + ")");
    jobQueue.add(job);
  }

  @Override
  public Collection<JobInProgress> getJobs() {
    return jobQueue;
  }

  /*
   * (non-Javadoc)
   * @seeorg.apache.hama.bsp.TaskScheduler#assignTasks(org.apache.hama.bsp.
   * GroomServerStatus)
   */
  @Override
  public List<Task> assignTasks(GroomServerStatus groomStatus)
      throws IOException {
    ClusterStatus clusterStatus = groomServerManager.getClusterStatus(false);

    final int numGroomServers = clusterStatus.getGroomServers();
    // final int clusterTaskCapacity = clusterStatus.getMaxTasks();

    //
    // Get task counts for the current groom.
    //
    // final int groomTaskCapacity = groom.getMaxTasks();
    final int groomRunningTasks = groomStatus.countTasks();

    // Assigned tasks
    List<Task> assignedTasks = new ArrayList<Task>();

    // Task task = null;
    if (groomRunningTasks == 0) {
      // TODO - Each time a job is submitted in BSPMaster, add a JobInProgress
      // instance to the scheduler.
      synchronized (jobQueue) {
        for (JobInProgress job : jobQueue) {
          if (job.getStatus().getRunState() != JobStatus.RUNNING) {
            continue;
          }

          Task t = null;

          t = job.obtainNewTask(groomStatus, numGroomServers,
              groomServerManager.getNumberOfUniqueHosts());
          if (t != null) {
            assignedTasks.add(t);
            break; // TODO - Now, simple scheduler assigns only one task to
            // each groom. Later, it will be improved for scheduler to
            // assign one or more tasks to each groom according to
            // its capacity.
          }
        }
      }
    }

    return assignedTasks;
  }
}
