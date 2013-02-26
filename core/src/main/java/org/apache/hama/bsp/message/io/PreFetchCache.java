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
package org.apache.hama.bsp.message.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class PreFetchCache<M extends Writable> {
  private static final Log LOG = LogFactory.getLog(PreFetchCache.class);
  private Object[] objectListArr;
  private long totalMessages;
  private int capacity;
  private int arrIndex;
  private int listIndex;
  private PreFetchThread<M> preFetchThread;
  private SpilledDataReadStatus status;
  private BitSet bufferBitSet;

  private static class PreFetchThread<M extends Writable> extends Thread
      implements Runnable {

    private volatile boolean stopReading;

    SpilledDataInputBuffer stream;
    Class<M> objectClass;
    private Object[] listArr;
    SpilledDataReadStatus status;
    private long totalMsgs;
    private int listCapacity;
    private int messageCount;
    private Configuration conf;

    private void fill(int index) throws IOException {
      @SuppressWarnings("unchecked")
      List<? super Writable> list = (List<? super Writable>) listArr[index];

      for (int i = 0; i < listCapacity && messageCount < totalMsgs
          && !stopReading; ++i) {

        if (i == list.size()) {
          if (objectClass == null) {
            ObjectWritable writable = new ObjectWritable();
            writable.readFields(stream);
            list.add(i, writable);
          } else {
            M obj = ReflectionUtils.newInstance(objectClass, conf);
            obj.readFields(stream);
            list.add(i, obj);
          }
        } else {
          Writable obj = (Writable) list.get(i);
          obj.readFields(stream);
        }
        ++messageCount;
      }

    }

    public PreFetchThread(Class<M> classObj, Object[] objectListArr,
        int capacity, SpilledDataInputBuffer inStream, long totalMessages,
        SpilledDataReadStatus indexStatus, Configuration conf) {
      objectClass = classObj;
      listArr = objectListArr;
      status = indexStatus;
      this.conf = conf;
      totalMsgs = totalMessages;
      messageCount = 0;
      this.stream = inStream;
      listCapacity = capacity;
    }

    @Override
    public void run() {
      int index = 0;
      try {
        while ((index = status.getFileBufferIndex()) >= 0 && !stopReading) {
          fill(index);
          if (stopReading || messageCount == totalMsgs) {
            status.closedBySpiller();
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted reading pre-fetch buffer index.", e);
      } catch (IOException e) {
        LOG.error("Error reading pre-fetch buffer index.", e);
      }
      status.completeReading();
    }

    public synchronized void stopReading() {
      stopReading = true;
    }

  }

  public PreFetchCache(long totalMessages) {
    this(2, totalMessages, 64);
  }

  public PreFetchCache(int numBuffers, long totalMessages) {
    this(numBuffers, totalMessages, 64);
  }

  public PreFetchCache(int numBuffers, long totalMessages, int capacity) {
    this.objectListArr = new Object[numBuffers];
    this.totalMessages = totalMessages;
    this.bufferBitSet = new BitSet();
    status = new SpilledDataReadStatus(numBuffers, bufferBitSet);
    for (int i = 0; i < numBuffers; ++i) {
      this.objectListArr[i] = new ArrayList<M>(capacity);
    }
    this.capacity = capacity;
  }

  public void startFetching(Class<M> classObject,
      SpilledDataInputBuffer buffer, Configuration conf)
      throws InterruptedException, IOException {

    preFetchThread = new PreFetchThread<M>(classObject, objectListArr,
        capacity, buffer, totalMessages, status, conf);
    preFetchThread.start();
    if (!status.startReading()) {
      throw new IOException("Failed to start reading the spilled file: ");
    }
    arrIndex = status.getReadBufferIndex();
  }

  @SuppressWarnings("unchecked")
  public Writable get() {
    if (listIndex == capacity) {
      try {
        arrIndex = status.getReadBufferIndex();
      } catch (InterruptedException e) {
        LOG.error("Interrupted getting prefetched records.", e);
        return null;
      }
      if (arrIndex < 0) {
        return null;
      }
      listIndex = 0;
    }
    return ((List<M>) (this.objectListArr[arrIndex])).get(listIndex++);
  }

  public void close() {
    status.completeReading();
    this.preFetchThread.stopReading();
    try {
      this.preFetchThread.join();
    } catch (InterruptedException e) {
      LOG.error("Prefetch thread was interrupted.", e);
    }
  }

}
