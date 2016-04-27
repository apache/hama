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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.TaskInProgress;
import org.apache.hama.bsp.BSPJobClient.RawSplit;

/**
 * <code>RoundRobinTaskAllocator</code> is a round robin based task allocator that equally
 * divides the tasks among all the Grooms. It balances the load of cluster. For example
 * if a cluster has 10 Grooms and 20 tasks are to be scheduled then each Groom which
 * get 2 tasks.
 */
public class RoundRobinTaskAllocator implements TaskAllocationStrategy {

  Log LOG = LogFactory.getLog(RoundRobinTaskAllocator.class);

  @Override
  public void initialize(Configuration conf) {
  }

  /**
   * This function loops through the whole list of Grooms with their task count
   * and returns the first Groom which contains the minimum number of tasks.
   * @param groomStatuses The map of groom-name to
   *          <code>GroomServerStatus</code> object for all known grooms.
   * @param taskCountInGroomMap Map of count of tasks in groom (To be deprecated
   *          soon)
   * @return returns the groom name which should be allocated the next task or 
   *          null no suitable groom was found.
   */
  private String findGroomWithMinimumTasks(
      Map<String, GroomServerStatus> groomStatuses,
      Map<GroomServerStatus, Integer> taskCountInGroomMap) {
    
    Entry<GroomServerStatus, Integer> firstGroomWithMinimumTasks = null;
    
    // At the start taskCountInGroomMap is empty so we have to put 0 tasks on grooms
    if (taskCountInGroomMap.size() < groomStatuses.size()) {
      for (String s : groomStatuses.keySet()) {
        GroomServerStatus groom = groomStatuses.get(s);
        if (groom == null)
          continue;
        Integer taskInGroom = taskCountInGroomMap.get(groom);
        
        // Find the groom that is yet to get its first tasks and assign 0 value to it.
        // Having zero will make sure that it gets selected.
        if (taskInGroom == null) {
          taskCountInGroomMap.put(groom, 0);
          break;
        }
      }
    }
    
    for (Entry<GroomServerStatus, Integer> currentGroom : taskCountInGroomMap.entrySet()) {
      if (firstGroomWithMinimumTasks == null || firstGroomWithMinimumTasks.getValue() > currentGroom.getValue()) {
        if(currentGroom.getValue() < currentGroom.getKey().getMaxTasks()) { // Assign the task to groom which still has space for more tasks 
          firstGroomWithMinimumTasks = currentGroom;
        } // If there is no space then continue and find the next best groom
      }
    }
    
    return (firstGroomWithMinimumTasks == null) ? null
        : firstGroomWithMinimumTasks.getKey().getGroomHostName();
  }
  
  /**
   * Select grooms that has the block of data locally stored on the groom
   * server.
   */
  @Override
  public String[] selectGrooms(Map<String, GroomServerStatus> groomStatuses,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      BSPResource[] resources, TaskInProgress taskInProgress) {
    if (!taskInProgress.canStartTask()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot start task based on id");
      }
      return new String[0];
    }

    RawSplit rawSplit = taskInProgress.getFileSplit();
    if (rawSplit != null) {
      return rawSplit.getLocations();
    }
    return null;
  }

  @Override
  public GroomServerStatus getGroomToAllocate(
      Map<String, GroomServerStatus> groomStatuses, String[] selectedGrooms,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      BSPResource[] resources, TaskInProgress taskInProgress) {
    if (!taskInProgress.canStartTask()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exceeded allowed attempts.");
      }
      return null;
    }    

    String groomName = null;

    groomName = findGroomWithMinimumTasks(groomStatuses, taskCountInGroomMap);

    if (groomName != null) {
      return groomStatuses.get(groomName);
    }
    
    return null;
  }

  /**
   * This operation is not supported.
   */
  @Override
  public Set<GroomServerStatus> getGroomsToAllocate(
      Map<String, GroomServerStatus> groomStatuses, String[] selectedGrooms,
      Map<GroomServerStatus, Integer> taskCountInGroomMap,
      BSPResource[] resources, TaskInProgress taskInProgress) {
    throw new UnsupportedOperationException(
        "This API is not supported for the called API function call.");
  }
}
