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

import org.apache.hadoop.io.LongWritable;

/**
 * Represents a directive from the {@link org.apache.hama.bsp.BSPMaster} to the
 * {@link org.apache.hama.bsp.GroomServer} to launch a new task.
 */
public class RecoverTaskAction extends GroomServerAction {
  private Task task;
  private LongWritable superstepNumber;

  public RecoverTaskAction() {
    super(ActionType.RECOVER_TASK);
    superstepNumber = new LongWritable(-1L);
  }

  public RecoverTaskAction(Task task, long superstep) {
    super(ActionType.RECOVER_TASK);
    this.task = task;
    this.superstepNumber = new LongWritable(superstep);
  }

  public Task getTask() {
    return task;
  }
  
  public long getSuperstepCount(){
    return superstepNumber.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    task.write(out);
    superstepNumber.write(out);
    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    task = new BSPTask();
    task.readFields(in);
    superstepNumber.readFields(in);
  }

}
