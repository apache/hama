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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Status information on the current state of the BSP cluster.
 * 
 * <p><code>ClusterStatus</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster. 
 *   </li>
 *   <li>
 *   Name of the grooms. 
 *   </li>
 *   <li>
 *   Task capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently running bsp tasks.
 *   </li>
 *   <li>
 *   State of the <code>BSPMaster</code>.
 *   </li>
 * </ol></p>
 * 
 * <p>Clients can query for the latest <code>ClusterStatus</code>, via 
 * {@link BSPJobClient#getClusterStatus(boolean)}.</p>
 * 
 * @see BSPMaster
 */
public class ClusterStatus implements Writable {

  private int numActiveGrooms;
  private Map<String, String> activeGrooms = new HashMap<String, String>();
  private int tasks;
  private int maxTasks;
  private BSPMaster.State state;
  
  /**
   * 
   */
  public ClusterStatus() {}
    
  public ClusterStatus(int grooms, int tasks, int maxTasks, BSPMaster.State state) {
    this.numActiveGrooms = grooms;
    this.tasks = tasks;
    this.maxTasks = maxTasks;
    this.state = state;
  }
  
  public ClusterStatus(Map<String, String> activeGrooms, int tasks, int maxTasks,
      BSPMaster.State state) {
    this(activeGrooms.size(), tasks, maxTasks, state);
    this.activeGrooms = activeGrooms;
  }
  
  /**
   * Get the number of groom servers in the cluster.
   * 
   * @return the number of groom servers in the cluster.
   */
  public int getGroomServers() {
    return numActiveGrooms;
  }
  
  /**
   * Get the names of groom servers, and their peers, in the cluster.
   * 
   * @return the active groom servers in the cluster.
   */  
  public Map<String, String> getActiveGroomNames() {
    return activeGrooms;
  }
  
  /**
   * Get the number of currently running tasks in the cluster.
   * 
   * @return the number of currently running tasks in the cluster.
   */
  public int getTasks() {
    return tasks;
  }
  
  /**
   * Get the maximum capacity for running tasks in the cluster.
   * 
   * @return the maximum capacity for running tasks in the cluster.
   */
  public int getMaxTasks() {
    return maxTasks;
  }
  
  /**
   * Get the current state of the <code>BSPMaster</code>, 
   * as {@link BSPMaster.State}
   * 
   * @return the current state of the <code>BSPMaster</code>.
   */
  public BSPMaster.State getBSPMasterState() {
    return state;
  }
  
  //////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    if (activeGrooms.isEmpty()) {
      out.writeInt(numActiveGrooms);
      out.writeBoolean(false);
    } else {
      out.writeInt(activeGrooms.size());
      out.writeBoolean(true);

      String[] groomNames = activeGrooms.keySet().toArray(new String[0]);
      List<String> peerNames = new ArrayList<String>();

      for (String groomName : groomNames) {
        peerNames.add(activeGrooms.get(groomName));
      }

      WritableUtils.writeCompressedStringArray(out, groomNames);
      WritableUtils.writeCompressedStringArray(out, peerNames.toArray(new String[0]));
    }
    out.writeInt(tasks);
    out.writeInt(maxTasks);
    WritableUtils.writeEnum(out, state);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    numActiveGrooms = in.readInt();
    boolean groomListFollows = in.readBoolean();

    if (groomListFollows) {
      String[] groomNames = WritableUtils.readCompressedStringArray(in);
      String[] peerNames = WritableUtils.readCompressedStringArray(in);
      activeGrooms = new HashMap<String, String>(groomNames.length);

      for (int i = 0; i < groomNames.length; i++) {
        activeGrooms.put(groomNames[i], peerNames[i]);
      }
    }

    tasks = in.readInt();
    maxTasks = in.readInt();
    state = WritableUtils.readEnum(in, BSPMaster.State.class);
  }
}
