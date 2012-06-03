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

package org.apache.hama.monitor.fd;

/**
 * A failure detector component. It is responsible for receiving the 
 * heartbeat and output suspicion level for Interpreter.
 */
public interface Supervisor {

  /**
   * Receive notification if a node fails.
   * @param listener will be called if a node fails.
   */
  void register(NodeEventListener listener);
  
  /**
   * Start supervisor.
   */
  void start();

  /**
   * Shutdown supervisor.
   */
  void stop();

}
