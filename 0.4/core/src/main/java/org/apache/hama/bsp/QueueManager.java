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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;

/**
 * A BSPJob Queue Manager. 
 */
public class QueueManager{

  private ConcurrentMap<String, Queue<JobInProgress>> queues = 
    new ConcurrentHashMap<String, Queue<JobInProgress>>();

  public QueueManager(Configuration conf){ }

  /**
   * Initialize a job.
   * @param job required initialzied.
   */
  public void initJob(JobInProgress job){
    try{
      //job.updateStatus();
      job.initTasks();
    }catch(IOException ioe){
      ioe.printStackTrace();
    }
  }

  /**
   * Add a job to the specified queue.
   * @param name of the queue.
   * @param job to be added.
   */
  public void addJob(String name, JobInProgress job){
    Queue<JobInProgress> queue = queues.get(name);
    if(null != queue) queue.addJob(job);
  }

  /**
   * Remove a job from the head of a designated queue.
   * @param name from which a job is removed.
   * @param job to be removed from the queue.
   */
  public void removeJob(String name, JobInProgress job){
    Queue<JobInProgress> queue = queues.get(name);
    if(null != queue) queue.removeJob(job);
  }

  /**
   * Move a job from a queue to another. 
   * @param from a queue a job is to be removed.
   * @param to a queue a job is to be added.
   */
  public void moveJob(String from, String to, JobInProgress job){
    synchronized(queues){
      removeJob(from, job);
      addJob(to, job);
    }  
  }

  /**
   * Create a FCFS queue with the name provided.
   * @param name of the queue. 
   */
  public void createFCFSQueue(String name){
    queues.putIfAbsent(name, new FCFSQueue(name));
  }

  /**
   * Find Queue according to the name specified.
   * @param name of the queue. 
   * @return queue of JobInProgress 
   */
  public Queue<JobInProgress> findQueue(String name){
     return queues.get(name);
  }

}
