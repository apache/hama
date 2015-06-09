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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;


/**
 * manager responsible for spawning new worker units for each job.
 */
public abstract class TaskWorkerManager implements Configurable{
  /**
   * An execution unit that will delegate a job for execution
   */
  public interface TaskWorker extends Callable<Boolean>{
  }

  protected Configuration conf;
  AtomicReference<GroomServerManager> groomServerManager = null;

  @Override
  public Configuration getConf() {
    return conf;
  }

  //TODO: Make this require conf for safty
  public void init(AtomicReference<GroomServerManager> groomServerManager, 
		  Configuration conf) {
    this.groomServerManager = groomServerManager;
    setConf(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public abstract TaskWorker spawnWorker(JobInProgress jip);
}
