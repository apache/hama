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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Simple queue interface.
 */
public interface MessageQueue<M extends Writable> extends Iterable<M>,
    Configurable {

  public static final String PERSISTENT_QUEUE = "hama.queue.behaviour.persistent";

  /**
   * Used to initialize the queue.
   * 
   * @param conf
   * @param id
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
   * 
   * @param col
   */
  public void addAll(Iterable<M> col);

  /**
   * Adds the other queue to this queue.
   * 
   * @param otherqueue
   */
  public void addAll(MessageQueue<M> otherqueue);

  /**
   * Adds a single item to the implementing queue.
   * 
   * @param item
   */
  public void add(M item);

  /**
   * Adds a bundle to the queue.
   * 
   * @param bundle
   */
  public void add(BSPMessageBundle<M> bundle);

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

  /**
   * 
   * @return true if the messages in the queue are serialized to byte buffers.
   */
  public boolean isMessageSerialized();

  /**
   * @return true if the queue is memory resident.
   */
  public boolean isMemoryBasedQueue();

}
