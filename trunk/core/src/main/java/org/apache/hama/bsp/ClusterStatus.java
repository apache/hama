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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Status information on the current state of the BSP cluster.
 * 
 * <p>
 * <code>ClusterStatus</code> provides clients with information such as:
 * <ol>
 * <li>
 * Size of the cluster.</li>
 * <li>
 * Name of the grooms.</li>
 * <li>
 * Task capacity of the cluster.</li>
 * <li>
 * The number of currently running bsp tasks.</li>
 * <li>
 * State of the <code>BSPMaster</code>.</li>
 * </ol>
 * </p>
 * 
 * <p>
 * Clients can query for the latest <code>ClusterStatus</code>, via
 * {@link BSPJobClient#getClusterStatus(boolean)}.
 * </p>
 * 
 * @see BSPMaster
 */
public class ClusterStatus implements Writable {

  private int numActiveGrooms;
  private Map<String, GroomServerStatus> activeGrooms = new HashMap<String, GroomServerStatus>();
  private Map<String, String> cachedActiveGroomNames = null;
  private int tasks;
  private int maxTasks;
  private BSPMaster.State state;

  /**
   * 
   */
  public ClusterStatus() {
  }

  public ClusterStatus(int grooms, int tasks, int maxTasks,
      BSPMaster.State state) {
    this.numActiveGrooms = grooms;
    this.tasks = tasks;
    this.maxTasks = maxTasks;
    this.state = state;
  }

  public ClusterStatus(Map<String, GroomServerStatus> activeGrooms, int tasks,
      int maxTasks, BSPMaster.State state) {
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
   * Get the names of groom servers, and their hostnames, in the cluster.
   * 
   * @return the active groom servers in the cluster.
   */
  public Map<String, String> getActiveGroomNames() {
    if (cachedActiveGroomNames == null) {
      if (activeGrooms != null) {
        Map<String, String> map = new HashMap<String, String>();
        for (Entry<String, GroomServerStatus> entry : activeGrooms.entrySet()) {
          map.put(entry.getKey(), entry.getValue().getGroomHostName());
        }
        cachedActiveGroomNames = map;
      }
    }
    return cachedActiveGroomNames;
  }

  /**
   * Get the names of groom servers, and their current status in the cluster.
   * 
   * @return the active groom servers in the cluster.
   */
  public Map<String, GroomServerStatus> getActiveGroomServerStatus() {
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
   * Get the current state of the <code>BSPMaster</code>, as
   * {@link BSPMaster.State}
   * 
   * @return the current state of the <code>BSPMaster</code>.
   */
  public BSPMaster.State getBSPMasterState() {
    return state;
  }

  // ////////////////////////////////////////////
  // Writable
  // ////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    if (activeGrooms.isEmpty()) {
      out.writeInt(numActiveGrooms);
      out.writeBoolean(false);
    } else {
      out.writeInt(activeGrooms.size());
      out.writeBoolean(true);

      for (Entry<String, GroomServerStatus> entry : activeGrooms.entrySet()) {
        out.writeUTF(entry.getKey());
        entry.getValue().write(out);
      }

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
      activeGrooms = new HashMap<String, GroomServerStatus>(numActiveGrooms);

      for (int i = 0; i < numActiveGrooms; i++) {
        final String groomName = in.readUTF();
        final GroomServerStatus status = new GroomServerStatus();
        status.readFields(in);
        activeGrooms.put(groomName, status);
      }
    }

    tasks = in.readInt();
    maxTasks = in.readInt();
    state = WritableUtils.readEnum(in, BSPMaster.State.class);
  }
}
