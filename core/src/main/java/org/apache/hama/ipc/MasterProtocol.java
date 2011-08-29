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

import org.apache.hama.bsp.Directive;
import org.apache.hama.bsp.GroomServerStatus;

/**
 * A new protocol for GroomServers communicate with BSPMaster. This
 * protocol paired with WorkerProtocl, let GroomServers enrol with 
 * BSPMaster, so that BSPMaster can dispatch tasks to GroomServers.
 */
public interface MasterProtocol extends HamaRPCProtocolVersion {

  /**
   * A GroomServer register with its status to BSPMaster, which will update
   * GroomServers cache.
   *
   * @param status to be updated in cache.
   * @return true if successfully register with BSPMaster; false if fail.
   */
  boolean register(GroomServerStatus status) throws IOException;

  /**
   * A GroomServer (periodically) reports task statuses back to the BSPMaster.
   * @param directive 
   */
  boolean report(Directive directive) throws IOException;

  public String getSystemDir();

}
