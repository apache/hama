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
package org.apache.hama.bsp.message;

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * A global mutex based synchronized queue.
 */
public final class SynchronizedQueue<T> {

  private final MessageQueue<T> queue;
  private final Object mutex;

  private SynchronizedQueue(MessageQueue<T> queue) {
    this.queue = queue;
    this.mutex = new Object();
  }

  private SynchronizedQueue(MessageQueue<T> queue, Object mutex) {
    this.queue = queue;
    this.mutex = mutex;
  }

  public Iterator<T> iterator() {
    synchronized (mutex) {
      return queue.iterator();
    }
  }

  public void setConf(Configuration conf) {
    synchronized (mutex) {
      queue.setConf(conf);
    }
  }

  public Configuration getConf() {
    synchronized (mutex) {
      return queue.getConf();
    }
  }

  public void init(Configuration conf, TaskAttemptID id) {
    synchronized (mutex) {
      queue.init(conf, id);
    }
  }

  public void close() {
    synchronized (mutex) {
    }
    queue.close();
  }

  public void prepareRead() {
    synchronized (mutex) {
      queue.prepareRead();
    }
  }

  public void addAll(Collection<T> col) {
    synchronized (mutex) {
      queue.addAll(col);
    }
  }

  public void add(T item) {
    synchronized (mutex) {
      queue.add(item);
    }
  }

  public void clear() {
    synchronized (mutex) {
      queue.clear();
    }
  }

  public Object poll() {
    synchronized (mutex) {
      return queue.poll();
    }
  }

  public int size() {
    synchronized (mutex) {
      return queue.size();
    }
  }

  public MessageQueue<T> getMessageQueue() {
    synchronized (mutex) {
      return queue;
    }
  }

  /*
   * static constructor methods to be type safe
   */

  public static <T> SynchronizedQueue<T> synchronize(MessageQueue<T> queue) {
    return new SynchronizedQueue<T>(queue);
  }

  public static <T> SynchronizedQueue<T> synchronize(MessageQueue<T> queue,
      Object mutex) {
    return new SynchronizedQueue<T>(queue, mutex);
  }

}
