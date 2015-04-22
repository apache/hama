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
package org.apache.hama.graph;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.message.queue.SynchronizedQueue;

public class IncomingVertexMessageManager<M extends WritableComparable<M>>
    implements SynchronizedQueue<GraphJobMessage> {

  private Configuration conf;

  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<WritableComparable, GraphJobMessage> storage = new ConcurrentHashMap<WritableComparable, GraphJobMessage>();
  private final ConcurrentLinkedQueue<GraphJobMessage> mapMessages = new ConcurrentLinkedQueue<GraphJobMessage>();

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void addBundle(BSPMessageBundle<GraphJobMessage> bundle) {
    addAll(bundle);
  }

  @Override
  public void addAll(Iterable<GraphJobMessage> col) {
    for (GraphJobMessage m : col)
      add(m);
  }

  @Override
  public void addAll(MessageQueue<GraphJobMessage> otherqueue) {
    GraphJobMessage poll = null;
    while ((poll = otherqueue.poll()) != null) {
      add(poll);
    }
  }

  @Override
  public void add(GraphJobMessage item) {
    if (item.isVertexMessage()) {
      if (!storage.containsKey(item.getVertexId())) {
        storage.putIfAbsent(item.getVertexId(), item);
      } else {
        storage.get(item.getVertexId()).addValuesBytes(item.getValuesBytes(),
            item.size());
      }
    } else {
      mapMessages.add(item);
    }
  }

  @Override
  public void clear() {
    mapMessages.clear();
    storage.clear();
  }

  Iterator<GraphJobMessage> it;

  @Override
  public GraphJobMessage poll() {
    if (mapMessages.size() > 0) {
      return mapMessages.poll();
    } else {
      if (storage.size() > 0 && it.hasNext()) {
        GraphJobMessage m = it.next();
        it.remove();
        return m;
      } else {
        return null;
      }
    }
  }

  @Override
  public int size() {
    return storage.size() + mapMessages.size();
  }

  // empty, not needed to implement
  @Override
  public void init(Configuration conf, TaskAttemptID id) {
  }

  @Override
  public void close() {
    this.clear();
  }

  @Override
  public MessageQueue<GraphJobMessage> getMessageQueue() {
    return this;
  }

  @Override
  public void prepareRead() {
    it = storage.values().iterator();
  }

}
