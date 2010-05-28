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

/**************************************************
 * A JobProfile tracks job's status
 * 
 **************************************************/
public class JobProfile implements Writable {

  static { // register a ctor
    WritableFactories.setFactory(JobProfile.class, new WritableFactory() {
      public Writable newInstance() {
        return new JobProfile();
      }
    });
  }

  String user;
  final BSPJobID jobid;
  String jobFile;
  String name;

  /**
   * Construct an empty {@link JobProfile}.
   */
  public JobProfile() {
    jobid = new BSPJobID();
  }

  /**
   * Construct a {@link JobProfile} the userid, jobid, job config-file,
   * job-details url and job name.
   * 
   * @param user userid of the person who submitted the job.
   * @param jobid id of the job.
   * @param jobFile job configuration file.
   * @param name user-specified job name.
   */
  public JobProfile(String user, BSPJobID jobid, String jobFile, String name) {
    this.user = user;
    this.jobid = jobid;
    this.jobFile = jobFile;
    this.name = name;
  }

  /**
   * Get the user id.
   */
  public String getUser() {
    return user;
  }

  /**
   * Get the job id.
   */
  public BSPJobID getJobID() {
    return jobid;
  }

  /**
   * Get the configuration file for the job.
   */
  public String getJobFile() {
    return jobFile;
  }

  /**
   * Get the user-specified job name.
   */
  public String getJobName() {
    return name;
  }

  // /////////////////////////////////////
  // Writable
  // /////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    jobid.write(out);
    Text.writeString(out, jobFile);
    Text.writeString(out, user);
    Text.writeString(out, name);
  }

  public void readFields(DataInput in) throws IOException {
    jobid.readFields(in);
    this.jobFile = Text.readString(in);
    this.user = Text.readString(in);
    this.name = Text.readString(in);
  }
}
