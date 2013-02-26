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
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * ByteBufferOutputStream encapsulates a byte buffer to write data into. The
 * function {@link ByteBufferOutputStream#onBufferFull(byte[], int, int)} should
 * be overriden to handle the case when the size of data exceeds the size of
 * buffer. The default behavior is to throw an exception.
 */
class ByteBufferOutputStream extends OutputStream {

  private final byte[] b = new byte[1];
  private byte[] interBuffer;
  private int interBufferDataSize;
  protected ByteBuffer buffer;

  public void clear() {
    if (this.buffer != null) {
      this.buffer.clear();
    }
    interBufferDataSize = 0;
  }

  /**
   * Sets the buffer for the stream.
   * 
   * @param buffer byte buffer to hold within.
   */
  public void setBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
    this.interBufferDataSize = 0;
    int interSize = Math.min(buffer.capacity() / 2, 8192);
    if (interBuffer == null) {
      interBuffer = new byte[interSize];
    }

  }

  @Override
  public void write(int b) throws IOException {
    if (interBufferDataSize < interBuffer.length - 1) {
      interBuffer[interBufferDataSize++] = (byte) (b & 0xFF);
      return;
    }

    this.b[0] = (byte) (b & 0xFF);
    write(this.b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len >= interBuffer.length) {
      /*
       * If the request length exceeds the size of the output buffer, flush the
       * output buffer and then write the data directly. In this way buffered
       * streams will cascade harmlessly.
       */
      flushBuffer();
      writeInternal(b, off, len);
      return;
    }
    if (len > interBuffer.length - interBufferDataSize) {
      flushBuffer();
    }
    System.arraycopy(b, off, interBuffer, interBufferDataSize, len);
    interBufferDataSize += len;
  }

  private void writeInternal(byte[] b, int off, int len) throws IOException {

    if (len <= buffer.remaining() || onBufferFull(b, off, len)) {
      buffer.put(b, off, len);
    }
  }

  /**
   * Action to take when the data to be written exceeds the size of the byte
   * buffer inside.
   * 
   * @return
   * @throws IOException
   */
  protected boolean onBufferFull(byte[] b, int off, int len) throws IOException {
    return true;
  }

  @Override
  public void flush() throws IOException {
    flushBuffer();
    onFlush();
  }

  /**
   * Called when the byte buffer stream is closed.
   * 
   * @throws IOException
   */
  protected void onFlush() throws IOException {

  }

  /** Flush the internal buffer */
  private void flushBuffer() throws IOException {
    if (interBufferDataSize > 0) {
      writeInternal(interBuffer, 0, interBufferDataSize);
      interBufferDataSize = 0;
    }
  }

  public ByteBuffer getBuffer() {
    return this.buffer;
  }

}
