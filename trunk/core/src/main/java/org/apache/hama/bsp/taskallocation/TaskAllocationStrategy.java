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
package org.apache.hama.bsp.taskallocation;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.TaskInProgress;

/**
 * <code>TaskAllocationStrategy</class> defines the behavior of task allocation
 * strategy that is employed by a Hama job to schedule tasks in GroomServers in 
 * Hama cluster. The function <code>selectGrooms</code> is responsible to define
 * the strategy to select the grooms. This list of grooms could be used in the
 * functions <code>getGroomToAllocate</code> or <code>getGroomsToAllocate</code>
 * in the parameter <code>selectedGrooms</code>. The two functions are not given
 * the responsibility to select grooms because these functions could also be
 * handling the task of allocating tasks on any other restricted set of grooms
 * that the caller invokes them for.
 * 
 */

@Unstable
public interface TaskAllocationStrategy {

  /**
   * Initializes the <code>TaskAllocationStrategy</code> instance.
   * 
   * @param conf Hama configuration
   */
  public abstract void initialize(Configuration conf);

  /**
   * Defines the task-allocation strategy to select the grooms based on the
   * resource constraints and the task related restrictions posed. This function
   * could be used to populate the <code>selectedGrooms</code> argument in the
   * functions <code>getGroomToAllocate</code> and
   * <code>getGroomsToAllocate</code>
   * 
   * @param groomStatuses The map of groom-name to
   *          <code>GroomServerStatus</code> object for all known grooms.
   * @param taskCountInGroomMap Map of count of tasks in groom (To be deprecated
   *          soon)
   * @param taskInProgress The <code>TaskInProgress</code> object for the task.
   * @return An array of hostnames where the tasks could be allocated on.
   */
  @Unstable
  public abstract String[] selectGrooms(
      Map<String, GroomServerStatus> groomStatuses,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      BSPResource[] resources, TaskInProgress taskInProgress);

  /**
   * Returns the best groom to run the task on based on the set of grooms
   * provided. The designer of the class can choose to populate the
   * <code>selectedGrooms</code> value with the function
   * <code>selectGrooms</code>
   * 
   * @param groomStatuses The map of groom-name to
   *          <code>GroomServerStatus</code> object for all known grooms.
   * @param selectedGrooms An array of selected groom host-names to select from.
   * @param taskCountInGroomMap Map of count of tasks in groom (To be deprecated
   *          soon)
   * @param taskInProgress The <code>TaskInProgress</code> object for the task.
   * @return Host Name of the selected groom. Returns null if no groom could be
   *         found.
   */
  @Unstable
  public abstract GroomServerStatus getGroomToAllocate(
      Map<String, GroomServerStatus> groomStatuses, String[] selectedGrooms,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      BSPResource[] resources, TaskInProgress taskInProgress);

  /**
   * Returns the best grooms to run the task on based on the set of grooms
   * provided. The designer of the class can choose to populate the
   * <code>selectedGrooms</code> value with the function
   * <code>selectGrooms</code>
   * 
   * @param groomStatuses The map of groom-name to
   *          <code>GroomServerStatus</code> object for all known grooms.
   * @param selectedGrooms An array of selected groom host-names to select from.
   * @param taskCountInGroomMap Map of count of tasks in groom (To be deprecated
   *          soon)
   * @param taskInProgress The <code>TaskInProgress</code> object for the task.
   * @return Host Names of the selected grooms where the task could be
   *         allocated. Returns null if no groom could be found.
   */
  @Unstable
  public abstract Set<GroomServerStatus> getGroomsToAllocate(
      Map<String, GroomServerStatus> groomStatuses, String[] selectedGrooms,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      BSPResource[] resources, TaskInProgress taskInProgress);
}
