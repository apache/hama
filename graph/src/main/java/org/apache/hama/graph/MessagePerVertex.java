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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.io.WritableComparable;

public class MessagePerVertex {

  @SuppressWarnings("rawtypes")
  private final ConcurrentNavigableMap<WritableComparable, GraphJobMessage> storage = new ConcurrentSkipListMap<WritableComparable, GraphJobMessage>();

  public int size() {
    return storage.size();
  }

  public void clear() {
    storage.clear();
  }

  @SuppressWarnings("rawtypes")
  public void put(WritableComparable vertexId, GraphJobMessage graphJobMessage) {
    storage.put(vertexId, graphJobMessage);
  }

  public void add(WritableComparable<?> vertexID, GraphJobMessage msg) {
    if (storage.containsKey(vertexID)) {
      storage.get(vertexID).addValuesBytes(msg.getValuesBytes(), msg.size());
    } else {
      put(vertexID, msg);
    }
  }

  @SuppressWarnings("rawtypes")
  public boolean containsKey(WritableComparable vertexID) {
    return storage.containsKey(vertexID);
  }

  @SuppressWarnings("rawtypes")
  public GraphJobMessage get(WritableComparable vertexID) {
    return storage.get(vertexID);
  }

  public Iterator<GraphJobMessage> iterator() {
    return storage.values().iterator();
  }

  public GraphJobMessage pollFirstEntry() {
    return (storage.size() > 0) ? storage.pollFirstEntry().getValue() : null;
  }

}
