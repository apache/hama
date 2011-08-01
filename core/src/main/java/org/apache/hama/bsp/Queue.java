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

import java.util.Collection;

/**
 * Job Queue interface.
 *  
 * @param <T>
 */
public interface Queue<T>{

  /**
   * The queue name.
   * @return the name of current queue.
   */ 
  String getName();

  /**
   * Add a job to a queue.
   * @param job to be added to the queue.
   */
  void addJob(T job);

  /**
   * Remove a job from the queue.
   * @param job to be removed from the queue.
   */
  void removeJob(T job);

  /**
   * Get a job
   * @return job that is removed from the queue.
   */
  T removeJob();

  /**
   * Return all data stored in this queue.
   * @return Collection of jobs.
   */
  public Collection<T> jobs();

}
