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

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskInProgress;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.MasterSyncClient;
import org.apache.hama.bsp.sync.PeerSyncClient;
import org.apache.hama.bsp.taskallocation.TaskAllocationStrategy;

/**
 * <code>BSPFaultTolerantService</code> defines the fault tolerance service
 * behavior. The fault tolerance service is a feature of a running job and not
 * the system. A class defined on this behavior has the responsibility to create
 * two objects. The first object <code>FaultTolerantMasterService</code> is
 * used by the job at BSPMaster to handle fault tolerance related steps at the
 * master. The second object <code>FaultTolerantPeerService</code> is used to
 * define the behavior of object that would implement the fault tolerance
 * related steps for recovery inside <code>BSPPeer</code> (in each of the BSP
 * peers doing computations)
 */
public interface BSPFaultTolerantService<M extends Writable> {
  
  /**
   * The token by which a job can register its fault-tolerance service.
   */
  public static final String FT_SERVICE_CONF = "hama.ft.conf.class";

  /**
   * Creates the instance of <code>FaultTolerantMasterService</code> that would
   * handle fault-tolerance related steps at BSPMaster task scheduler.
   * 
   * @param jobId The identifier of the job.
   * @param maxTaskAttempts Number of attempts allowed for recovering from
   *          failure.
   * @param tasks The list of tasks in the job.
   * @param conf The job configuration object.
   * @param masterClient The synchronization client used by BSPMaster.
   * @param allocationStrategy The task allocation strategy of the job.
   * @return An instance of class inheriting
   *         <code>FaultTolerantMasterService</code>
   */
  public FaultTolerantMasterService constructMasterFaultTolerance(
      BSPJobID jobId, int maxTaskAttempts, TaskInProgress[] tasks,
      Configuration conf, MasterSyncClient masterClient,
      TaskAllocationStrategy allocationStrategy) throws Exception;

  /**
   * Creates an instance of <code>FaultTolerantPeerService</code> which defines
   * the steps that has to be taken inside a peer for fault-tolerance.
   * 
   * @param bspPeer The peer
   * @param syncClient The synchronization client used by peer.
   * @param superstep The superstep from which the peer is initialized.
   * @param conf job configuration object
   * @param messenger The messaging system between the peers
   * @return An instance of class inheriting
   *         <code>FaultTolerantPeerService</code>
   */
  public FaultTolerantPeerService<M> constructPeerFaultTolerance(BSPJob job,
      @SuppressWarnings("rawtypes")
      BSPPeer bspPeer, PeerSyncClient syncClient,
      InetSocketAddress peerAddress, TaskAttemptID taskAttemptId,
      long superstep, Configuration conf, MessageManager<M> messenger)
      throws Exception;

}
