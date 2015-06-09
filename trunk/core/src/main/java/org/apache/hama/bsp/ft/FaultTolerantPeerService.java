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
package org.apache.hama.bsp.ft;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskStatus;

/**
 * <code>FaultTolerantPeerService</code> defines the steps required to be
 * performed by peers for fault-tolerance. At different stages of peer
 * execution, the service can take necessary measures to ensure that the peer
 * computations could be recovered if any of them failed.
 */
public interface FaultTolerantPeerService<M extends Writable> {

  /**
   * This is called once the peer is initialized.
   * 
   * @throws Exception
   */
  public TaskStatus.State onPeerInitialized(TaskStatus.State state)
      throws Exception;

  /**
   * This function is called before all the peers go into global sync/
   * 
   * @throws Exception
   */
  public void beforeBarrier() throws Exception;

  /**
   * This functions is called after the peers enter the barrier but before they
   * initate leaving the barrier.
   * 
   * @throws Exception
   */
  public void duringBarrier() throws Exception;

  /**
   * This function is called every time the peer completes the global
   * synchronization.
   * 
   * @throws Exception
   */
  public void afterBarrier() throws Exception;

}
