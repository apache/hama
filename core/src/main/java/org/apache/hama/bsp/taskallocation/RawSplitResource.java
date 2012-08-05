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

import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.TaskInProgress;

/**
 * <code>RawSplitResource</code> defines the data block resource that could be
 * used to find which groom to schedule for data-locality. 
 */
public class RawSplitResource extends BSPResource{

  private RawSplit split;
  
  public RawSplitResource(){
    
  }
  
  /**
   * Initialize the resource with data block split information.
   * @param split The data-split provided by <code>BSPJobClient</client>
   */
  public RawSplitResource(RawSplit split){
    this.split = split;
  }
  
  @Override
  public String[] getGrooms(TaskInProgress tip) {
    return split.getLocations();
  }

}
