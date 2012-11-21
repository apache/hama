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
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Heap (Java's priority queue) based message queue implementation that supports
 * sorted receive and send.
 */
public final class SortedMessageQueue<M extends WritableComparable<M>>
    implements MessageQueue<M> {

  private final PriorityQueue<M> queue = new PriorityQueue<M>();
  private Configuration conf;

  @Override
  public Iterator<M> iterator() {
    return queue.iterator();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void addAll(Collection<M> col) {
    queue.addAll(col);
  }

  @Override
  public void addAll(MessageQueue<M> otherqueue) {
    M poll = null;
    while ((poll = otherqueue.poll()) != null) {
      queue.add(poll);
    }
  }

  @Override
  public void add(M item) {
    queue.add(item);
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public M poll() {
    return queue.poll();
  }

  @Override
  public int size() {
    return queue.size();
  }

  // empty, not needed to implement

  @Override
  public void init(Configuration conf, TaskAttemptID id) {

  }

  @Override
  public void close() {

  }

  @Override
  public void prepareRead() {

  }

  @Override
  public void prepareWrite() {

  }

}
