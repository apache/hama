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

public class TrackedRecordReader<K, V> implements RecordReader<K, V> {
  private RecordReader<K, V> rawIn;
  private Counters.Counter inputByteCounter;
  private Counters.Counter inputRecordCounter;
  private long beforePos = -1;
  private long afterPos = -1;

  TrackedRecordReader(RecordReader<K, V> raw,
      Counters.Counter inputRecordCounter, Counters.Counter inputByteCounter)
      throws IOException {
    rawIn = raw;
    this.inputRecordCounter = inputRecordCounter;
    this.inputByteCounter = inputByteCounter;
  }

  public K createKey() {
    return rawIn.createKey();
  }

  public V createValue() {
    return rawIn.createValue();
  }

  public synchronized boolean next(K key, V value) throws IOException {
    boolean ret = moveToNext(key, value);
    if (ret) {
      incrCounters();
    }
    return ret;
  }

  protected void incrCounters() {
    inputRecordCounter.increment(1);
    inputByteCounter.increment(afterPos - beforePos);
  }

  protected synchronized boolean moveToNext(K key, V value) throws IOException {
    beforePos = getPos();
    boolean ret = rawIn.next(key, value);
    afterPos = getPos();
    return ret;
  }

  public long getPos() throws IOException {
    return rawIn.getPos();
  }

  public void close() throws IOException {
    rawIn.close();
  }

  public float getProgress() throws IOException {
    return rawIn.getProgress();
  }
}
