/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Hadoop RPC based barrier synchronization service.
 * 
 */
public interface SyncServer extends VersionedProtocol {

  public static final long versionID = 0L;

  public void enterBarrier(TaskAttemptID id);

  public void leaveBarrier(TaskAttemptID id);

  public void register(TaskAttemptID id, Text hostAddress, LongWritable port);

  public LongWritable getSuperStep();

  public StringArrayWritable getAllPeerNames();

  public void deregisterFromBarrier(TaskAttemptID id, Text hostAddress,
      LongWritable port);

  public void stopServer();

}
