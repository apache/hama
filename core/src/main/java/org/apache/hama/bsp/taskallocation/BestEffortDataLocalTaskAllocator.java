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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.TaskInProgress;

/**
 * <code>BestEffortDataLocalTaskAllocator</code> is a simple task allocator that
 * takes in only the data locality as a constraint for allocating tasks. It
 * makes the best attempt to schedule task on the groom server with the input
 * split. If the aforesaid is not possible, it selects any other available groom
 * to allocate tasks on.
 */
public class BestEffortDataLocalTaskAllocator implements TaskAllocationStrategy {

  Log LOG = LogFactory.getLog(BestEffortDataLocalTaskAllocator.class);

  @Override
  public void initialize(Configuration conf) {
  }

  /**
   * Returns the first groom that has a slot to schedule a task on.
   * 
   * @param grooms
   * @param tasksInGroomMap
   * @return
   */
  private static String getAnyGroomToSchedule(Map<String, GroomServerStatus> grooms,
      Map<GroomServerStatus, Integer> tasksInGroomMap) {

    Iterator<String> groomIter = grooms.keySet().iterator();
    while (groomIter.hasNext()) {
      GroomServerStatus groom = grooms.get(groomIter.next());
      if (groom == null)
        continue;
      Integer taskInGroom = tasksInGroomMap.get(groom);
      taskInGroom = (taskInGroom == null) ? 0 : taskInGroom;
      if (taskInGroom < groom.getMaxTasks()) {
        return groom.getGroomHostName();
      }
    }
    return null;
  }

  /**
   * From the set of grooms given, returns the groom on which a task could be
   * scheduled on.
   * 
   * @param grooms
   * @param tasksInGroomMap
   * @param possibleLocations
   * @return
   */
  private String getGroomToSchedule(Map<String, GroomServerStatus> grooms,
      Map<GroomServerStatus, Integer> tasksInGroomMap,
      String[] possibleLocations) {

    for (int i = 0; i < possibleLocations.length; ++i) {
      String location = possibleLocations[i];
      GroomServerStatus groom = grooms.get(location);
      if (groom == null){
        if(LOG.isDebugEnabled()){
          LOG.debug("Could not find groom for location " + location);
        }
        continue;
      }
      Integer taskInGroom = tasksInGroomMap.get(groom);
      taskInGroom = (taskInGroom == null) ? 0 : taskInGroom;
      if(LOG.isDebugEnabled()){
        LOG.debug("taskInGroom = " + taskInGroom + " max tasks = " + groom.getMaxTasks()
            + " location = " + location + " groomhostname = " + groom.getGroomHostName());
      }
      if (taskInGroom < groom.getMaxTasks()
          && location.equals(groom.getGroomHostName())) {
        return groom.getGroomHostName();
      }
    }
    if(LOG.isDebugEnabled()){
      LOG.debug("Returning null");
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
    if (selectedGrooms != null) {
      groomName = getGroomToSchedule(groomStatuses, taskCountInGroomMap,
          selectedGrooms);
    }

    if (groomName == null) {
      groomName = getAnyGroomToSchedule(groomStatuses, taskCountInGroomMap);
    }

    if (groomName != null) {
      return groomStatuses.get(groomName);
    }

    return null;
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
