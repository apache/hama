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
package org.apache.hama.bsp.message.queue;

import java.util.Collection;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Simple queue interface.
 */
public interface MessageQueue<M> extends Iterable<M>, Configurable {

  /**
   * Used to initialize the queue.
   */
  public void init(Configuration conf, TaskAttemptID id);

  /**
   * Finally close the queue. Commonly used to free resources.
   */
  public void close();

  /**
   * Called to prepare a queue for reading.
   */
  public void prepareRead();

  /**
   * Called to prepare a queue for writing.
   */
  public void prepareWrite();

  /**
   * Adds a whole Java Collection to the implementing queue.
   */
  public void addAll(Collection<M> col);

  /**
   * Adds the other queue to this queue.
   */
  public void addAll(MessageQueue<M> otherqueue);

  /**
   * Adds a single item to the implementing queue.
   */
  public void add(M item);

  /**
   * Clears all entries in the given queue.
   */
  public void clear();

  /**
   * Polls for the next item in the queue (FIFO).
   * 
   * @return a new item or null if none are present.
   */
  public M poll();

  /**
   * @return how many items are in the queue.
   */
  public int size();

}
