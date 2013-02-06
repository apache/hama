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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <code>SpilledDataInputBuffer</code> class is designed to read from the
 * spilling buffer. Depending on whether the records were spilled or not, the
 * stream provides data from a list of byte arrays or from file. The contents of
 * the file are asynchronously loaded in the byte arrays as the values are read.
 * 
 */
public class SpilledDataInputBuffer extends DataInputStream implements
    DataInput {
  private static final Log LOG = LogFactory
      .getLog(SpilledDataInputBuffer.class);

  /**
   * The thread is used to asynchronously read from the spilled file and load
   * the buffer for the user to read from byte arrays in heap.
   */
  static class SpillReadThread implements Callable<Boolean> {

    private String fileName;
    private List<SpilledByteBuffer> bufferList_;
    private long bytesToRead_;
    private long bytesWrittenInFile_;
    private SpilledDataReadStatus status_;
    private boolean closed_;

    /**
     * Creates the thread to read the contents of the file and loads into the
     * list of byte arrays.
     * 
     * @param fileName Name of the file
     * @param bufferList list of byte arrays.
     * @param bytesInFile Total bytes in the file.
     * @param status The shared object that synchronizes the indexes for buffer
     *          to fill the data with.
     */
    public SpillReadThread(String fileName, List<SpilledByteBuffer> bufferList,
        SpilledDataReadStatus status) {
      this.fileName = fileName;
      bufferList_ = bufferList;
      status_ = status;
      closed_ = false;
    }

    /**
     * Keeps reading from file and loads the next available byte array with the
     * data from the file.
     * 
     * @throws IOException
     */
    private void keepReadingFromFile() throws IOException {
      RandomAccessFile raf = new RandomAccessFile(fileName, "r");
      FileChannel fc = raf.getChannel();
      bytesToRead_ = fc.size();
      bytesWrittenInFile_ = bytesToRead_;
      long fileReadPos = 0;
      int fileReadIndex = -1;
      do {
        try {
          fileReadIndex = status_.getFileBufferIndex();
        } catch (InterruptedException e1) {
          throw new IOException(e1);
        }

        if (fileReadIndex < 0)
          break;

        SpilledByteBuffer buffer = bufferList_.get(fileReadIndex);
        buffer.clear();
        long readSize = Math.min(buffer.remaining(),
            (bytesWrittenInFile_ - fileReadPos));
        readSize = fc.read(buffer.getByteBuffer());
        if (readSize < 0) {
          break;
        }
        buffer.flip();
        bytesToRead_ -= readSize;
        fileReadPos += readSize;

      } while (!closed_ && bytesToRead_ > 0 && fileReadIndex >= 0
          && fileReadIndex < bufferList_.size());
      fc.close();
      closed_ = true;
      status_.closedBySpiller();
    }

    /*
     * Indicate the thread to close.
     */
    public void completeRead() {
      closed_ = true;
    }

    public boolean isClosed() {
      return closed_;
    }

    @Override
    public Boolean call() throws Exception {
      try {
        keepReadingFromFile();
      } catch (Exception e) {
        LOG.error("Error reading from file: " + fileName, e);
        status_.notifyError();
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    }

  }

  /**
   * The input stream that encapsulates a previously written spilled buffer that
   * has a list of byte arrays and a file to read from in the case where
   * spilling is already done. If spilling has not happened, all the data is
   * present in the list of byte arrays.
   * 
   */
  static class SpilledInputStream extends InputStream {

    private String fileName_;
    private List<SpilledByteBuffer> bufferList_;
    private boolean spilledAlready_;
    ReadIndexStatus status_;
    private final byte[] readByte = new byte[1];
    private int count;
    private final byte[] buf;
    private int pos;
    private Callable<Boolean> spillReadThread_;
    private Future<Boolean> spillReadState_;
    private ExecutorService spillThreadService_;
    private SpilledByteBuffer currentReadBuffer_;
    private BitSet bufferBitState_;

    private boolean closed_;

    public SpilledInputStream(String fileName, boolean direct,
        List<SpilledByteBuffer> bufferList, boolean hasSpilled)
        throws IOException {
      fileName_ = fileName;
      bufferList_ = bufferList;
      spilledAlready_ = hasSpilled;
      bufferBitState_ = new BitSet(bufferList.size());

      if (spilledAlready_) {
        status_ = new SpilledDataReadStatus(bufferList.size(), bufferBitState_);
      } else {
        status_ = new BufferReadStatus(bufferList.size());
      }

      buf = new byte[8192];
      count = 0;
      pos = 0;
      closed_ = false;

    }

    public void prepareRead() throws IOException {
      if (spilledAlready_) {
        spillReadThread_ = new SpillReadThread(fileName_, bufferList_,
            (SpilledDataReadStatus) status_);
        spillThreadService_ = Executors.newFixedThreadPool(1);
        spillReadState_ = spillThreadService_.submit(spillReadThread_);
        if (!status_.startReading()) {
          throw new IOException("Failed to read the spilled file: " + fileName_);
        }
      }
      try {
        currentReadBuffer_ = getNextBuffer();
      } catch (InterruptedException e1) {
        throw new IOException(e1);
      }

      if (currentReadBuffer_ == null) {
        if (spilledAlready_) {
          try {
            spillReadState_.get();
          } catch (InterruptedException e) {
            throw new IOException(e);
          } catch (ExecutionException e) {
            throw new IOException(e);
          } finally {
            spillThreadService_.shutdownNow();
          }
        }
        throw new IOException("Could not initialize the buffer for reading");
      }
    }

    public SpilledByteBuffer getNextBuffer() throws InterruptedException {
      int index = status_.getReadBufferIndex();
      if (index >= 0 && index < bufferList_.size()) {
        return bufferList_.get(index);
      }
      return null;

    }

    @Override
    public int read() throws IOException {
      if (count > 0) {
        --count;
        return buf[pos++] & 0xFF;
      }

      if (-1 == read(readByte, 0, 1)) {
        return -1;
      }
      return readByte[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {

      if (count >= len) {
        // copy the count of bytes to b
        System.arraycopy(buf, pos, b, off, len);
        count -= len;
        pos += len;
        return len;
      }
      int size = 0;

      while (len > 0) {
        if (count == 0) {
          count = readInternal(buf, 0, buf.length);
          if (count == -1) {
            return count;
          }
          pos = 0;
        }
        int readSize = Math.min(count, len);
        System.arraycopy(buf, pos, b, off, readSize);
        len -= readSize;
        off += readSize;
        size += readSize;
        count -= readSize;
        pos += readSize;
      }
      return size;

    }

    public int readInternal(byte[] b, int off, int len) throws IOException {
      if (currentReadBuffer_ == null) {
        return -1;
      }
      int cur = 0;
      while (len > 0) {
        int rem = currentReadBuffer_.remaining();
        if (rem == 0) {
          try {
            currentReadBuffer_ = getNextBuffer();
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
          if (currentReadBuffer_ == null) {
            return cur;
          }
          rem = currentReadBuffer_.remaining();
        }
        int readSize = Math.min(rem, len);
        currentReadBuffer_.get(b, off, readSize);
        len -= readSize;
        off += rem;
        cur += readSize;
      }

      return cur;
    }

    public void clear() throws IOException {
      close();
      bufferBitState_.clear();

      if (spilledAlready_) {
        status_ = new SpilledDataReadStatus(bufferList_.size(), bufferBitState_);
      } else {
        status_ = new BufferReadStatus(bufferList_.size());
      }

      count = 0;
      pos = 0;
      closed_ = false;
      prepareRead();
    }

    @Override
    public void close() throws IOException {

      if (closed_)
        return;

      status_.completeReading();
      if (this.spilledAlready_) {
        try {
          this.spillReadState_.get();
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (ExecutionException e) {
          throw new IOException(e);
        } finally {
          spillThreadService_.shutdownNow();
        }
      }

    }

    public String getFileName() {
      return fileName_;
    }

  }

  public void completeReading(boolean deleteFile) throws IOException {
    in.close();
    if (deleteFile) {
      File file = new File(
          ((SpilledDataInputBuffer.SpilledInputStream) in).getFileName());
      if (file.exists())
        file.delete();
    }
  }

  public SpilledDataInputBuffer(InputStream in) {
    super(in);
  }

  public void clear() throws IOException {
    SpilledInputStream inStream = (SpilledInputStream) this.in;
    inStream.clear();
  }

  public static SpilledDataInputBuffer getSpilledDataInputBuffer(
      String fileName, boolean direct, List<SpilledByteBuffer> bufferList)
      throws IOException {
    SpilledInputStream inStream = new SpilledInputStream(fileName, direct,
        bufferList, true);
    inStream.prepareRead();
    return new SpilledDataInputBuffer(inStream);
  }

}
