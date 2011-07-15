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

import java.util.Collection;
import java.util.Map;

import org.apache.hama.ipc.GroomProtocol;

/**
 * Manages information about the {@link GroomServer}s in the cluster 
 * environment. This interface is not intended to be implemented by users.
 */
interface GroomServerManager {

  /**
   * Get the current status of the cluster
   * @param detailed if true then report groom names as well
   * @return summary of the state of the cluster
   */
  ClusterStatus getClusterStatus(boolean detailed);

  /**
   * Find WorkerProtocol with corresponded groom server status
   * 
   * @param groomId The identification value of GroomServer 
   * @return GroomServerStatus 
   */
  GroomProtocol findGroomServer(GroomServerStatus status);

  /**
   * Find the collection of groom servers.
   * 
   * @return Collection of groom servers list.
   */
  Collection<GroomProtocol> findGroomServers();

  /**
   * Collection of GroomServerStatus as the key set.
   *
   * @return Collection of GroomServerStatus.
   */
  Collection<GroomServerStatus> groomServerStatusKeySet();

  /**
   * Registers a JobInProgressListener to GroomServerManager. Therefore,
   * adding a JobInProgress will trigger the jobAdded function.
   * @param the JobInProgressListener listener to be added.
   */
  void addJobInProgressListener(JobInProgressListener listener);

  /**
   * Unregisters a JobInProgressListener to GroomServerManager. Therefore,
   * the remove of a JobInProgress will trigger the jobRemoved action.
   * @param the JobInProgressListener to be removed.
   */
  void removeJobInProgressListener(JobInProgressListener listener);

  /**
   * Current GroomServer Peers.
   * @return GroomName and PeerName(host:port) in pair. 
   */
  Map<String, String> currentGroomServerPeers();
}
