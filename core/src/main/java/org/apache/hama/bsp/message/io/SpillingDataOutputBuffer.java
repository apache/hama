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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.message.io.SpillWriteIndexStatus;
import org.apache.hama.bsp.message.io.SpilledDataInputBuffer;
import org.apache.hama.bsp.message.io.SpilledDataProcessor;
import org.apache.hama.bsp.message.io.SpillingDataOutputBuffer;
import org.apache.hama.bsp.message.io.WriteSpilledDataProcessor;

/**
 * <code>SpillingBuffer</code> is an output stream comprised of byte arrays that
 * keeps values in heap until a particular threshold is reached. Once this
 * threshold is exceeded, the values are spilled to disk and all the contents of
 * the buffer is written to a file until the stream is closed. The
 * implementation uses a list of byte arrays and hence a user of this class may
 * provide the size of each byte array to hold the data. The threshold could
 * also be specified provided it exceeds the size of a single byte array. Once
 * the stream is closed, the class provides an input stream to read the data
 * written which may or may not have been spilled.
 * 
 */
public class SpillingDataOutputBuffer extends DataOutputStream {

  private static final Log LOG = LogFactory.getLog(SpillingDataOutputBuffer.class);

  /**
   * This thread is responsible for writing from the ByteBuffers in the list to
   * the file as they get available.
   */
  static class ProcessSpilledDataThread implements Callable<Boolean> {
    private SpillWriteIndexStatus status_;
    private List<ByteBuffer> bufferList_;
    private long fileWrittenSize_;
    private boolean closed;
    SpilledDataProcessor processor;

    ProcessSpilledDataThread(SpillWriteIndexStatus status,
        List<ByteBuffer> bufferList, SpilledDataProcessor processor) {
      status_ = status;
      bufferList_ = bufferList;
      closed = false;
      this.processor = processor;
    }

    /**
     * Keep writing to the file as the buffers gets available.
     * 
     * @throws IOException when the thread is interrupted while waiting to get
     *           the index of the buffer to written to the file.
     */
    private void keepProcessingData() throws IOException {

      int fileWriteIndex = -1;
      do {

        try {
          fileWriteIndex = status_.getNextProcessorBufferIndex();
        } catch (InterruptedException e1) {
          throw new IOException(e1);
        }
        while (fileWriteIndex >= 0) {
          ByteBuffer buffer = bufferList_.get(fileWriteIndex);
          processor.handleSpilledBuffer(buffer);
          buffer.clear();
          try {
            fileWriteIndex = status_.getNextProcessorBufferIndex();
          } catch (InterruptedException e) {
            LOG.error("Interrupted getting next index to process data.", e);
            throw new IOException(e);
          }

        }
      } while (!closed);


    }

    /*
     * Indicate the thread that the spilling process is complete.
     */
    public void completeSpill() {
      closed = true;
    }

    /**
     * Gets the size of file written in bytes.
     * 
     * @return the size of file written.
     */
    public long getFileWrittenSize() {
      return fileWrittenSize_;
    }

    @Override
    public Boolean call() throws Exception {
      keepProcessingData();
      return Boolean.TRUE;
    }

  }

  /**
   * This class is responsible for holding the <code>ByteBuffer</code> arrays
   * and writing data into the buffers. Once the the threshold is crossed it
   * invokes a spilling thread that would spill the data from the buffer to the
   * disk.
   * 
   */
  static class SpillingStream extends OutputStream {

    final byte[] b;
    final boolean direct_;

    private List<ByteBuffer> bufferList_;
    private int bufferSize_;
    private BitSet bufferState_;
    private int numberBuffers_;
    private ByteBuffer currentBuffer_;
    private long bytesWritten_;
    private long bytesRemaining_;
    private SpillWriteIndexStatus spillStatus_;
    private int thresholdSize_;
    private boolean startedSpilling_;
    private ProcessSpilledDataThread spillThread_;
    private ExecutorService spillThreadService_;
    private Future<Boolean> spillThreadState_;
    private boolean closed_;

    private SpilledDataProcessor processor;
    /**
     * The internal buffer where data is stored.
     */
    protected byte buf[];

    /**
     * The number of valid bytes in the buffer. This value is always in the
     * range <tt>0</tt> through <tt>buf.length</tt>; elements <tt>buf[0]</tt>
     * through <tt>buf[count-1]</tt> contain valid byte data.
     */
    protected int count;

    /**
     * Default intermediate buffer size;
     */
    protected int defaultBufferSize_;

    /**
     * 
     * @param numBuffers The number of ByteBuffer the class should hold
     * @param bufferSize The size of each ByteBuffer.
     * @param threshold The threshold after which the spilling should start.
     * @param direct true indicates the ByteBuffer be allocated direct
     * @param fileName The name of the file where the spilled data should be
     *          written into.
     */
    SpillingStream(int numBuffers, int bufferSize, int threshold,
        boolean direct, SpilledDataProcessor processor) {
      this(numBuffers, bufferSize, threshold, direct, processor, 8192);

    }

    /**
     * 
     * @param numBuffers The number of ByteBuffer the class should hold
     * @param bufferSize The size of each ByteBuffer.
     * @param threshold The threshold after which the spilling should start.
     * @param direct true indicates the ByteBuffer be allocated direct
     * @param fileName The name of the file where the spilled data should be
     *          written into.
     */
    SpillingStream(int numBuffers, int bufferSize, int threshold,
        boolean direct, SpilledDataProcessor processor, int interBufferSize) {

      assert (threshold >= bufferSize);
      assert (threshold < numBuffers * bufferSize);
      if(interBufferSize > bufferSize){
        interBufferSize = bufferSize/2;
      }
      defaultBufferSize_ = interBufferSize;
      this.b = new byte[1];
      this.buf = new byte[defaultBufferSize_];
      count = 0;
      direct_ = direct;
      numberBuffers_ = numBuffers;
      bufferSize_ = bufferSize;
      bufferList_ = new ArrayList<ByteBuffer>(numberBuffers_);
      bufferState_ = new BitSet(numBuffers);

      for (int i = 0; i < numBuffers / 2; ++i) {
        if (direct_) {
          bufferList_.add(ByteBuffer.allocateDirect(bufferSize_));
        } else {
          bufferList_.add(ByteBuffer.allocate(bufferSize_));
        }
      }
      currentBuffer_ = bufferList_.get(0);
      bytesWritten_ = 0L;
      bytesRemaining_ = bufferSize_;
      spillStatus_ = new SpillWriteIndexStatus(bufferSize, numberBuffers_, 0,
          -1, bufferState_);
      thresholdSize_ = threshold;
      startedSpilling_ = false;
      spillThread_ = null;
      spillThreadState_ = null;
      this.processor = processor;
      closed_ = false;

    }
    
    public void clear() throws IOException{
      this.close();
      startedSpilling_ = false;
      bufferState_.clear();

      for (int i = 0; i < bufferList_.size(); ++i) {
        bufferList_.get(i).clear();
      }
      currentBuffer_ = bufferList_.get(0);
      bytesWritten_ = 0L;
      bytesRemaining_ = bufferSize_;
    }

    @Override
    public void write(int b) throws IOException {
      if (count < buf.length - 1) {
        buf[count++] = (byte) (b & 0xFF);
        return;
      }
      
      this.b[0] = (byte) (b & 0xFF);
      write(this.b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    /**
     * Keep track of the data written to the buffer. If it exceeds the threshold
     * start the spilling thread.
     * 
     * @param len
     * @throws InterruptedException
     */
    private void startSpilling() throws InterruptedException {
      synchronized (this) {
        spillThread_ = new ProcessSpilledDataThread(spillStatus_, bufferList_,
            processor);
        startedSpilling_ = true;
        spillThreadService_ = Executors.newFixedThreadPool(1);
        spillThreadState_ = spillThreadService_.submit(spillThread_);
        spillStatus_.startSpilling();
      }
      // }
    }

    public void perfectFillWrite(byte[] b, int off, int len) throws IOException {
      int rem = currentBuffer_.remaining();
      while (len > rem) {
        currentBuffer_.put(b, off, rem);
        // bytesWritten_ += len;
        // if (bytesWritten_ > thresholdSize_ && !startedSpilling_) {
        // try {
        // startSpilling(rem);
        // } catch (InterruptedException e) {
        // throw new IOException("Internal error occured writing to buffer.",
        // e);
        // }
        // }

        currentBuffer_.flip();
        int index = spillStatus_.getNextBufferIndex();
        currentBuffer_ = getBuffer(index);
        off += rem;
        len -= rem;
        rem = currentBuffer_.remaining();
      }
      currentBuffer_.put(b, off, len);
      // bytesWritten_ += len;
      // if (bytesWritten_ > thresholdSize_ && !startedSpilling_) {
      // try {
      // startSpilling(len);
      // } catch (InterruptedException e) {
      // throw new IOException("Internal error occured writing to buffer.", e);
      // }
      // }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (len >= buf.length) {
        /*
         * If the request length exceeds the size of the output buffer, flush
         * the output buffer and then write the data directly. In this way
         * buffered streams will cascade harmlessly.
         */
        flushBuffer();
        writeInternal(b, off, len);
        return;
      }
      if (len > buf.length - count) {
        flushBuffer();
      }
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    private void writeInternal(byte[] b, int off, int len) throws IOException {

      bytesWritten_ += len;
      if (bytesWritten_ >= thresholdSize_ && !startedSpilling_) {
        try {
          startSpilling();
        } catch (InterruptedException e) {
          throw new IOException("Internal error occured writing to buffer.", e);
        }
      }

      if (len > bytesRemaining_) {
        currentBuffer_.flip();
        currentBuffer_ = getBuffer(spillStatus_.getNextBufferIndex());
        bytesRemaining_ = bufferSize_;
      }
      currentBuffer_.put(b, off, len);
      bytesRemaining_ -= len;

    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
      if (count > 0) {
        writeInternal(buf, 0, count);
        count = 0;
      }
    }

    /**
     * Gets the ByteBuffer from the buffer list.
     * 
     * @param index
     * @return
     * @throws IOException
     */
    ByteBuffer getBuffer(int index) throws IOException {

      if (index >= bufferList_.size()) {
        if (direct_) {
          bufferList_.add(index, ByteBuffer.allocateDirect(bufferSize_));
        } else {
          bufferList_.add(index, ByteBuffer.allocate(bufferSize_));
        }
      }

      return bufferList_.get(index);
    }

    /**
     * Closes the spilling process.
     */
    public void flush() throws IOException {
      flushBuffer();
      flushInternal();
    }

    public void flushInternal() throws IOException {
      if (closed_)
        return;

      currentBuffer_.flip();
      spillStatus_.spillCompleted();
      if (this.startedSpilling_) {
        this.spillThread_.completeSpill();
        boolean completionState = false;
        try {
          completionState = spillThreadState_.get();
          if (!completionState) {
            throw new IOException(
                "Spilling Thread failed to complete sucessfully.");
          }
        } catch (ExecutionException e) {
          throw new IOException(e);
        } catch (InterruptedException e) {
          throw new IOException(e);
        } finally {
          closed_ = true;
          this.processor.close();
          this.spillThreadService_.shutdownNow();
        }
        
      }
    }

  }

  /**
   * Initialize the spilling buffer with spilling file name
   * 
   * @param fileName name of the file.
   * @throws FileNotFoundException 
   */
  public SpillingDataOutputBuffer(String fileName) throws FileNotFoundException {
    super(new SpillingStream(3, 16 * 1024, 16 * 1024, true, 
        new WriteSpilledDataProcessor(fileName)));
  }
  
  public SpillingDataOutputBuffer(SpilledDataProcessor processor) throws FileNotFoundException {
    super(new SpillingStream(3, 16 * 1024, 16 * 1024, true, 
        processor));
  }
  
  

  /**
   * Initializes the spilling buffer.
   * @throws FileNotFoundException 
   */
  public SpillingDataOutputBuffer() throws FileNotFoundException {
    super(new SpillingStream(3, 16 * 1024, 16 * 1024, true,
        new WriteSpilledDataProcessor(
        System.getProperty("java.io.tmpdir") + File.separatorChar
            + new BigInteger(128, new SecureRandom()).toString(32))));
  }

  /**
   * 
   * @param bufferCount
   * @param bufferSize
   * @param threshold
   * @param direct
   * @param fileName
   */
  public SpillingDataOutputBuffer(int bufferCount, int bufferSize, int threshold,
      boolean direct, SpilledDataProcessor processor) {
    super(new SpillingStream(bufferCount, bufferSize, threshold, direct,
        processor));
  }
  
  public void clear() throws IOException{
    SpillingStream stream = (SpillingStream) this.out;
    stream.clear();
  }
  
  public boolean hasSpilled(){
    return ((SpillingStream) this.out).startedSpilling_;
  }

  /**
   * Provides an input stream to read from the spilling buffer.
   * 
   * @throws IOException
   */
  public SpilledDataInputBuffer getInputStreamToRead(String fileName) throws IOException {

    SpillingStream stream = (SpillingStream) this.out;
    SpilledDataInputBuffer.SpilledInputStream inStream = new SpilledDataInputBuffer.SpilledInputStream(
        fileName, stream.direct_, stream.bufferList_,
        stream.startedSpilling_);
    inStream.prepareRead();
    return new SpilledDataInputBuffer(inStream);
  }

}
