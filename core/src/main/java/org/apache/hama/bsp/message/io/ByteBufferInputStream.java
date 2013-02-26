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
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * <code>ByteBufferInputStream<code> is used to read back from SpilledByteBuffer. The
 * goal of this class to read data till a boundary record marked. The remaining
 * data which is the partial record in the byte buffer is read into the intermediate 
 * buffer using {@link ByteBufferInputStream#fillForNext()}. If this stream is 
 * to be used as an intermediate buffer to read from a bigger source of data, 
 * say a file, the function {@link ByteBufferInputStream#onBufferRead(byte[], int, int, int)}
 * could be used to fill up the ByteBuffer encapsulated by the class.
 * 
 */
public class ByteBufferInputStream extends InputStream {

  private final byte[] readByte = new byte[1];
  private int interBuffSize;
  private byte[] interBuffer;
  private int dataSizeInInterBuffer;
  private int curPos;

  protected ByteBuffer buffer;
  private long toBeRead;
  private long bytesRead;
  private long totalBytes;

  public ByteBufferInputStream() {
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  public boolean hasDataToRead() {
    return ((toBeRead - bytesRead) > 0);
  }

  public boolean hasUnmarkedData() {
    return ((totalBytes - bytesRead) > 0);
  }

  @Override
  public int read() throws IOException {
    if (dataSizeInInterBuffer > 0) {
      --dataSizeInInterBuffer;
      ++bytesRead;
      return interBuffer[curPos++] & 0xFF;
    }

    if (-1 == read(readByte, 0, 1)) {
      return -1;
    }
    return readByte[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (dataSizeInInterBuffer >= len) {
      // copy the count of bytes to b
      System.arraycopy(interBuffer, curPos, b, off, len);
      dataSizeInInterBuffer -= len;
      curPos += len;
      bytesRead += len;
      return len;
    }
    int size = 0;

    while (len > 0) {
      if (dataSizeInInterBuffer == 0) {
        dataSizeInInterBuffer = readInternal(interBuffer, 0, interBuffer.length);
        curPos = 0;
        if (dataSizeInInterBuffer <= 0) {
          break;
        }

      }
      int readSize = Math.min(dataSizeInInterBuffer, len);
      System.arraycopy(interBuffer, curPos, b, off, readSize);
      len -= readSize;
      off += readSize;
      size += readSize;
      dataSizeInInterBuffer -= readSize;
      curPos += readSize;
    }
    bytesRead += size;
    return size;
  }

  public int readInternal(byte[] b, int off, int len) throws IOException {
    if (buffer == null) {
      return -1;
    }
    int cur = 0;
    while (len > 0) {
      int rem = buffer.remaining();
      if (rem == 0) {
        return onBufferRead(b, off, len, cur);
      }
      int readSize = Math.min(rem, len);
      buffer.get(b, off, readSize);
      len -= readSize;
      off += readSize;
      cur += readSize;
    }
    return cur;
  }

  /**
   * When the byte buffer encapsulated is out of data then this function is
   * invoked.
   * 
   * @param b the byte buffer to read into
   * @param off offset index to start writing
   * @param len length of data to be written
   * @param cur The current size already read by the class.
   * @return if the end of the stream has reached, and cur is 0 return -1; else
   *         return the data size currently read.
   * @throws IOException
   */
  protected int onBufferRead(byte[] b, int off, int len, int cur)
      throws IOException {
    if (cur != 0)
      return cur;
    else
      return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  /**
   * Sets the byte buffer to read the data from.
   * 
   * @param buffer The byte buffer to read data from
   * @param toRead Number of bytes till end of last record.
   * @param total Total bytes of data to read in the buffer.
   * @throws IOException
   */
  public void setBuffer(ByteBuffer buffer, long toRead, long total)
      throws IOException {
    this.buffer = buffer;
    toBeRead = toRead += dataSizeInInterBuffer;
    totalBytes = total;
    bytesRead = 0;
    if (interBuffer == null) {
      interBuffSize = Math.min(buffer.remaining(), 8192);
      interBuffer = new byte[interBuffSize];
      dataSizeInInterBuffer = 0;
    }
    fetchIntermediate();
  }

  private void fetchIntermediate() throws IOException {
    int readSize = readInternal(interBuffer, dataSizeInInterBuffer,
        interBuffer.length - dataSizeInInterBuffer);
    if (readSize > 0) {
      dataSizeInInterBuffer += readSize;
    }
    curPos = 0;
  }

  /**
   * This function should be called to provision reading the partial records
   * into the buffer after the last record in the buffer is read. This data
   * would be appended with the next ByteBuffer that is set using
   * {@link ByteBufferInputStream#setBuffer(ByteBuffer, long, long)} to start
   * reading records.
   * 
   * @throws IOException
   */
  public void fillForNext() throws IOException {

    int remainingBytes = buffer.remaining();
    if (curPos != 0) {
      System.arraycopy(interBuffer, curPos, interBuffer, 0,
          dataSizeInInterBuffer);
    }
    curPos = 0;
    if (remainingBytes == 0)
      return;

    if (dataSizeInInterBuffer + remainingBytes > interBuffSize) {
      interBuffSize = dataSizeInInterBuffer + remainingBytes;
      byte[] arr = this.interBuffer;
      this.interBuffer = new byte[interBuffSize];
      System.arraycopy(arr, 0, interBuffer, 0, dataSizeInInterBuffer);
    }
    int readSize = readInternal(this.interBuffer, dataSizeInInterBuffer,
        remainingBytes);
    if (readSize > 0)
      dataSizeInInterBuffer += readSize;
  }
}
