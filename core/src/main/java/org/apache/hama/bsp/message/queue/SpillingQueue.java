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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.bundle.BSPMessageBundle;
import org.apache.hama.bsp.message.bundle.HeapByteArrayBSPMessageBundle;
import org.apache.hama.bsp.message.io.CombineSpilledDataProcessor;
import org.apache.hama.bsp.message.io.PreFetchCache;
import org.apache.hama.bsp.message.io.SpilledDataInputBuffer;
import org.apache.hama.bsp.message.io.SpilledDataProcessor;
import org.apache.hama.bsp.message.io.SpillingDataOutputBuffer;

/**
 * 
 * 
 * @param <M>
 */
public class SpillingQueue<M extends Writable> extends ByteArrayMessageQueue<M> {

  private static final Log LOG = LogFactory.getLog(SpillingQueue.class);

  private Configuration conf;
  private SpillingDataOutputBuffer spillOutputBuffer;
  private int numMessagesWritten;
  private int numMessagesRead;

  public final static String SPILLBUFFER_COUNT = "hama.io.spillbuffer.count";
  public final static String SPILLBUFFER_SIZE = "hama.io.spillbuffer.size";
  public final static String SPILLBUFFER_FILENAME = "hama.io.spillbuffer.filename";
  public final static String SPILLBUFFER_THRESHOLD = "hama.io.spillbuffer.threshold";
  public final static String SPILLBUFFER_DIRECT = "hama.io.spillbuffer.direct";
  public final static String ENABLE_PREFETCH = "hama.io.spillbuffer.enableprefetch";
  public final static String SPILLBUFFER_MSGCLASS = "hama.io.spillbuffer.msgclass";
  private int bufferCount;
  private int bufferSize;
  private String fileName;
  private int threshold;
  private boolean direct;
  private SpilledDataInputBuffer spilledInput;
  private boolean objectWritableMode;
  private ObjectWritable objectWritable;

  private Class<M> messageClass;
  private PreFetchCache<M> prefetchCache;
  private boolean enablePrefetch;

  private class SpillIterator implements Iterator<M> {

    private boolean objectMode;
    private Class<M> classObject;
    private M messageHolder;

    public SpillIterator(boolean mode, Class<M> classObj, Configuration conf) {
      this.objectMode = mode;
      this.classObject = classObj;
      if (classObject != null) {
        messageHolder = ReflectionUtils.newInstance(classObj, conf);
      }
    }

    @Override
    public boolean hasNext() {
      return numMessagesRead != numMessagesWritten && numMessagesWritten > 0;
    }

    @Override
    public M next() {
      if (objectMode) {
        return poll();
      } else {
        return poll(messageHolder);
      }
    }

    @Override
    public void remove() {
      // do nothing
    }

  }

  @Override
  public Iterator<M> iterator() {
    return new SpillIterator(objectWritableMode, messageClass, conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.objectWritable.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void add(M msg) {
    try {
      if (objectWritableMode) {
        objectWritable.set(msg);
        objectWritable.write(spillOutputBuffer);
      } else {
        msg.write(spillOutputBuffer);
      }
      spillOutputBuffer.markRecordEnd();
      ++numMessagesWritten;
    } catch (IOException e) {
      LOG.error("Error adding message.", e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public void addAll(Iterable<M> msgs) {
    for (M msg : msgs) {
      add(msg);
    }

  }

  @Override
  public void addAll(MessageQueue<M> arg0) {
    for (M anArg0 : arg0) {
      add(anArg0);
    }
  }

  @Override
  public void clear() {
    try {
      spillOutputBuffer.close();
      spillOutputBuffer.clear();
      spilledInput.close();
      spilledInput.clear();
    } catch (IOException e) {
      LOG.error("Error clearing spill stream.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {

    try {
      this.spillOutputBuffer.close();
      if (this.spilledInput != null) {
        this.spilledInput.close();
        this.spilledInput.completeReading(true);
      }
    } catch (IOException e) {
      LOG.error("Error closing the spilled input stream.", e);
      throw new RuntimeException(e);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Configuration conf, TaskAttemptID arg1) {

    bufferCount = conf.getInt(SPILLBUFFER_COUNT, 3);
    bufferSize = conf.getInt(SPILLBUFFER_SIZE, Constants.BUFFER_DEFAULT_SIZE);
    direct = conf.getBoolean(SPILLBUFFER_DIRECT, true);
    threshold = conf.getInt(SPILLBUFFER_THRESHOLD,
        Constants.BUFFER_DEFAULT_SIZE);
    fileName = conf.get(SPILLBUFFER_FILENAME,
        System.getProperty("java.io.tmpdir") + File.separatorChar
            + new BigInteger(128, new SecureRandom()).toString(32));

    messageClass = (Class<M>) conf.getClass(Constants.MESSAGE_CLASS, null);
    objectWritableMode = messageClass == null;

    SpilledDataProcessor processor;
    try {
      processor = new CombineSpilledDataProcessor<M>(fileName);
      processor.init(conf);
    } catch (FileNotFoundException e) {
      LOG.error("Error initializing spilled data stream.", e);
      throw new RuntimeException(e);
    }
    spillOutputBuffer = new SpillingDataOutputBuffer(bufferCount, bufferSize,
        threshold, direct, processor);
    objectWritable = new ObjectWritable();
    objectWritable.setConf(conf);
    this.conf = conf;
  }

  private void incReadMsgCount() {
    ++numMessagesRead;
  }

  @SuppressWarnings("unchecked")
  private M readDirect(M msg) {
    if (numMessagesRead >= numMessagesWritten) {
      return null;
    }
    try {
      if (objectWritableMode) {
        objectWritable.readFields(spilledInput);
        incReadMsgCount();
        return (M) objectWritable.get();
      } else {
        msg.readFields(spilledInput);
        incReadMsgCount();
        return msg;
      }
    } catch (IOException e) {
      LOG.error("Error getting values from spilled input", e);
    }
    return null;
  }

  public M poll(M msg) {
    if (numMessagesRead >= numMessagesWritten) {
      return null;
    }
    if (enablePrefetch) {
      return readFromPrefetch(msg);
    } else {
      return readDirect(msg);
    }
  }

  @SuppressWarnings("unchecked")
  private M readDirectObjectWritable() {
    if (!objectWritableMode) {
      throw new IllegalStateException(
          "API call not supported. Set the configuration property "
              + "'hama.io.spillbuffer.newmsginit' to true.");
    }
    try {
      objectWritable.readFields(spilledInput);
      incReadMsgCount();
    } catch (IOException e) {
      LOG.error("Error getting values from spilled input", e);
      return null;
    }
    return (M) objectWritable.get();
  }

  @SuppressWarnings({ "unchecked" })
  private M readFromPrefetch(M msg) {
    if (objectWritableMode) {
      this.objectWritable = (ObjectWritable) prefetchCache.get();
      incReadMsgCount();
      return (M) this.objectWritable.get();
    } else {
      incReadMsgCount();
      return (M) this.prefetchCache.get();
    }

  }

  @Override
  public M poll() {
    if (numMessagesRead >= numMessagesWritten) {
      return null;
    }

    if (enablePrefetch) {
      M msg = readFromPrefetch(null);
      if (msg != null)
        incReadMsgCount();
      return msg;
    } else {
      return readDirectObjectWritable();
    }
  }

  @Override
  public void prepareRead() {
    try {
      spillOutputBuffer.close();
    } catch (IOException e) {
      LOG.error("Error closing spilled buffer", e);
      throw new RuntimeException(e);
    }
    try {
      spilledInput = spillOutputBuffer.getInputStreamToRead(fileName);
    } catch (IOException e) {
      LOG.error("Error initializing the input spilled stream", e);
      throw new RuntimeException(e);
    }
    if (conf.getBoolean(ENABLE_PREFETCH, false)) {
      this.prefetchCache = new PreFetchCache<M>(numMessagesWritten);
      this.enablePrefetch = true;
      try {
        this.prefetchCache.startFetching(this.messageClass, spilledInput, conf);
      } catch (InterruptedException e) {
        LOG.error("Error starting prefetch on message queue.", e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void prepareWrite() {
    numMessagesWritten = 0;
  }

  @Override
  public int size() {
    return numMessagesWritten - numMessagesRead;
  }

  @Override
  public boolean isMessageSerialized() {
    return true;
  }

  @Override
  public void add(BSPMessageBundle<M> bundle) {
    try {
      this.spillOutputBuffer.write(((HeapByteArrayBSPMessageBundle<M>) bundle)
          .getBuffer());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isMemoryBasedQueue() {
    return false;
  }

}
