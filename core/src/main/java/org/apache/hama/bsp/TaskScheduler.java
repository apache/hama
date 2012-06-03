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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Used by a {@link BSPMaster} to schedule {@link Task}s on {@link GroomServer}
 * s.
 */
abstract class TaskScheduler implements Configurable {

  protected Configuration conf;
  protected final AtomicReference<GroomServerManager> groomServerManager = 
    new AtomicReference<GroomServerManager>(null);
  protected final AtomicReference<MonitorManager> monitorManager = 
    new AtomicReference<MonitorManager>(null);

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public void setGroomServerManager(final GroomServerManager groomServerManager) {
    this.groomServerManager.set(groomServerManager);
  }

  public void setMonitorManager(final MonitorManager monitorManager) {
    this.monitorManager.set(monitorManager);
  }

  /**
   * Lifecycle method to allow the scheduler to start any work in separate
   * threads.
   * 
   * @throws IOException
   */
  public void start() throws IOException {
    // do nothing
  }

  /**
   * Lifecycle method to allow the scheduler to stop any work it is doing.
   * 
   * @throws IOException
   */
  public void terminate() throws IOException {
    // do nothing
  }

  /**
   * Returns a collection of jobs in an order which is specific to the
   * particular scheduler.
   * 
   * @param Queue name.
   * @return JobInProgress corresponded to the specified queue.
   */
  public abstract Collection<JobInProgress> getJobs(String queue);

  /**
   * Find a job according to its id.
   * @param id of the job.
   * @return job corresponded to the id provided.
   */
  public abstract JobInProgress findJobById(BSPJobID id); 
}
