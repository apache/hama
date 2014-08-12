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

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * LinkedList backed queue structure for bookkeeping messages.
 */
public final class MemoryQueue<M extends Writable> implements
    SynchronizedQueue<M>, MessageQueue<M> {

  private final ConcurrentLinkedQueue<M> messages = new ConcurrentLinkedQueue<M>();
  private final ConcurrentLinkedQueue<BSPMessageBundle<M>> bundles = new ConcurrentLinkedQueue<BSPMessageBundle<M>>();
  private Iterator<M> bundleIterator;

  int bundledMessageSize = 0;

  private Configuration conf;

  @Override
  public final void addAll(Iterable<M> col) {
    for (M m : col)
      messages.add(m);
  }

  @Override
  public void addAll(MessageQueue<M> otherqueue) {
    M poll = null;
    while ((poll = otherqueue.poll()) != null) {
      messages.add(poll);
    }
  }

  @Override
  public final void add(M item) {
    messages.add(item);
  }

  @Override
  public void add(BSPMessageBundle<M> bundle) {
    bundledMessageSize += bundle.size();
    bundles.add(bundle);
  }

  @Override
  public final void clear() {
    messages.clear();
    bundles.clear();
    bundleIterator = null;
  }

  @Override
  public final M poll() {
    if (messages.size() > 0) {
      return messages.poll();
    } else {
      if (bundles.size() > 0) {
        if (bundleIterator == null) {
          bundleIterator = bundles.poll().iterator();
        } else {
          if (!bundleIterator.hasNext()) {
            bundleIterator = bundles.poll().iterator();
          }
        }

        bundledMessageSize--;
        return bundleIterator.next();
      }
    }

    return null;
  }

  @Override
  public final int size() {
    return messages.size() + bundledMessageSize;
  }

  @Override
  public final Iterator<M> iterator() {
    Iterator<M> it = new Iterator<M>() {

      @Override
      public boolean hasNext() {
        if (size() > 0)
          return true;
        else
          return false;
      }

      @Override
      public M next() {
        return poll();
      }

      @Override
      public void remove() {
      }
    };
    return it;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  // not doing much here
  @Override
  public void init(Configuration conf, TaskAttemptID id) {

  }

  @Override
  public void close() {
    this.clear();
  }

  @Override
  public void prepareRead() {

  }

  @Override
  public void prepareWrite() {

  }

  @Override
  public boolean isMessageSerialized() {
    return false;
  }

  @Override
  public boolean isMemoryBasedQueue() {
    return true;
  }

  @Override
  public MessageQueue<M> getMessageQueue() {
    return this;
  }

}
