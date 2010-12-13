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

import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.HeartbeatResponse;

/**
 * Protocol that a GroomServer and the central BSPMaster use to communicate. The
 * BSPMaster is the Server, which implements this protocol.
 */
public interface InterServerProtocol extends HamaRPCProtocolVersion {
  public HeartbeatResponse heartbeat(GroomServerStatus status,
      boolean restarted, boolean initialContact, boolean acceptNewTasks,
      short responseId, int reportSize) throws IOException;

  public String getSystemDir();
}
