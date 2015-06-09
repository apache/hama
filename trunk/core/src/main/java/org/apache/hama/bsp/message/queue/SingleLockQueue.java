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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * A global mutex based synchronized queue.
 */
public final class SingleLockQueue<T extends Writable> implements
    SynchronizedQueue<T> {

  private final MessageQueue<T> queue;
  private final Object mutex;

  private SingleLockQueue(MessageQueue<T> queue) {
    this.queue = queue;
    this.mutex = new Object();
  }

  private SingleLockQueue(MessageQueue<T> queue, Object mutex) {
    this.queue = queue;
    this.mutex = mutex;
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.bsp.message.SynchronizedQueue#setConf(org.apache.hadoop
   * .conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    synchronized (mutex) {
      queue.setConf(conf);
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#getConf()
   */
  @Override
  public Configuration getConf() {
    synchronized (mutex) {
      return queue.getConf();
    }
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.bsp.message.SynchronizedQueue#init(org.apache.hadoop.conf
   * .Configuration, org.apache.hama.bsp.TaskAttemptID)
   */
  @Override
  public void init(Configuration conf, TaskAttemptID id) {
    synchronized (mutex) {
      queue.init(conf, id);
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#close()
   */
  @Override
  public void close() {
    synchronized (mutex) {
    }
    queue.close();
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.bsp.message.SynchronizedQueue#addAll(java.util.Collection)
   */
  @Override
  public void addAll(Iterable<T> col) {
    synchronized (mutex) {
      for (T m : col)
        queue.add(m);
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#add(T)
   */
  @Override
  public void add(T item) {
    synchronized (mutex) {
      queue.add(item);
    }
  }

  @Override
  public void addBundle(BSPMessageBundle<T> bundle) {
    synchronized (mutex) {
      queue.addBundle(bundle);
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#clear()
   */
  @Override
  public void clear() {
    synchronized (mutex) {
      queue.clear();
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#poll()
   */
  @Override
  public T poll() {
    synchronized (mutex) {
      return queue.poll();
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#size()
   */
  @Override
  public int size() {
    synchronized (mutex) {
      return queue.size();
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.SynchronizedQueue#getMessageQueue()
   */
  @Override
  public MessageQueue<T> getMessageQueue() {
    synchronized (mutex) {
      return queue;
    }
  }

  /*
   * static constructor methods to be type safe
   */
  public static <T extends Writable> SynchronizedQueue<T> synchronize(
      MessageQueue<T> queue) {
    return (SynchronizedQueue<T>) queue;
  }

  public static <T extends Writable> SynchronizedQueue<T> synchronize(
      MessageQueue<T> queue, Object mutex) {
    return new SingleLockQueue<T>(queue, mutex);
  }

  @Override
  public void addAll(MessageQueue<T> otherqueue) {
    synchronized (mutex) {
      queue.addAll(otherqueue);
    }
  }

  @Override
  public void prepareRead() {
    // TODO Auto-generated method stub
    
  }

}
