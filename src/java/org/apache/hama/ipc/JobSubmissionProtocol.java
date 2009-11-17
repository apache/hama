/**
 * Copyright 2008 The Apache Software Foundation
 *
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

import org.apache.hama.graph.JobID;
import org.apache.hama.graph.JobStatus;

/**
 * Protocol that a Walker and the central Master use to communicate. This
 * interface will contains several methods: submitJob, killJob, and killTask.
 */
public interface JobSubmissionProtocol extends HamaRPCProtocolVersion {
  public JobID getNewJobId() throws IOException;

  public JobStatus submitJob(JobID jobName) throws IOException;
}
