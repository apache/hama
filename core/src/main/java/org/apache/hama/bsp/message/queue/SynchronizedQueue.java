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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Synchronized Queue interface. Can be used to implement better synchronized
 * datastructures.
 */
public interface SynchronizedQueue<T> extends Configurable {

  public abstract Iterator<T> iterator();

  public abstract void init(Configuration conf, TaskAttemptID id);

  public abstract void close();

  public abstract void prepareRead();

  public abstract void addAll(Collection<T> col);

  public abstract void add(T item);

  public abstract void clear();

  public abstract Object poll();

  public abstract int size();

  public abstract MessageQueue<T> getMessageQueue();

}
