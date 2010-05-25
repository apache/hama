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

import java.io.IOException;

import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.JobProfile;
import org.apache.hama.bsp.JobStatus;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Protocol that a groom server and the central BSP Master use to communicate. This
 * interface will contains several methods: submitJob, killJob, and killTask.
 */
public interface JobSubmissionProtocol extends HamaRPCProtocolVersion {
  
  /**
   * Allocate a new id for the job.
   * @return
   * @throws IOException
   */
  public BSPJobID getNewJobId() throws IOException;
  
  
  /**
   * Submit a Job for execution.  Returns the latest profile for
   * that job. 
   * The job files should be submitted in <b>system-dir</b>/<b>jobName</b>.
   *
   * @param jobName
   * @return
   * @throws IOException
   */
  public JobStatus submitJob(BSPJobID jobName) throws IOException;
  
  /**
   * Get the current status of the cluster
   * @param detailed if true then report groom names as well
   * @return summary of the state of the cluster
   */
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException;
  
  /**
   * Grab a handle to a job that is already known to the BSPMaster.
   * @return Profile of the job, or null if not found. 
   */
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException;
  
  /**
   * Grab a handle to a job that is already known to the BSPMaster.
   * @return Status of the job, or null if not found.
   */
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException;
  
  /**
   * A BSP system always operates on a single filesystem.  This 
   * function returns the fs name.  ('local' if the localfs; 'addr:port' 
   * if dfs).  The client can then copy files into the right locations 
   * prior to submitting the job.
   */
  public String getFilesystemName() throws IOException;
  
  /** 
   * Get all the jobs submitted. 
   * @return array of JobStatus for the submitted jobs
   */
  public JobStatus[] getAllJobs() throws IOException;

  /**
   * Grab the bspmaster system directory path where job-specific files are to be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public String getSystemDir();
  
  /**
   * Kill the indicated job
   */
  public void killJob(BSPJobID jobid) throws IOException;
  
  
  /**
   * Kill indicated task attempt.
   * @param taskId the id of the task to kill.
   * @param shouldFail if true the task is failed and added to failed tasks list, otherwise
   * it is just killed, w/o affecting job failure status.  
   */ 
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException;  
  
}
