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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A {@link ByteBuffer} input stream that synchronously reads from spilled data.
 * Uses {@link DuplexByteArrayChannel} within.
 */
public class SyncReadByteBufferInputStream extends ByteBufferInputStream {

  private static final Log LOG = LogFactory
      .getLog(SyncReadByteBufferInputStream.class);

  private boolean spilled;
  private FileChannel fileChannel;
  private long fileBytesToRead;
  private long fileBytesRead;
  private DuplexByteArrayChannel duplexChannel = new DuplexByteArrayChannel();

  public SyncReadByteBufferInputStream(boolean isSpilled, String fileName) {
    spilled = isSpilled;
    if (isSpilled) {
      RandomAccessFile f;
      try {
        f = new RandomAccessFile(fileName, "r");
        fileChannel = f.getChannel();
        fileBytesToRead = fileChannel.size();
      } catch (FileNotFoundException e) {
        LOG.error("File not found initializing Synchronous Input Byte Stream",
            e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        LOG.error("Error initializing Synchronous Input Byte Stream", e);
        throw new RuntimeException(e);
      }

    }
  }

  private void feedDataFromFile() throws IOException {
    int toReadNow = (int) Math.min(buffer.capacity(), fileBytesToRead);
    fileChannel.transferTo(fileBytesRead, toReadNow, duplexChannel);
    fileBytesRead += toReadNow;
    fileBytesToRead -= toReadNow;
    duplexChannel.flip();
  }

  @Override
  public void setBuffer(ByteBuffer buffer, long toRead, long total)
      throws IOException {
    this.buffer = buffer;
    duplexChannel.setBuffer(buffer);
    if (spilled) {
      feedDataFromFile();
    }
    super.setBuffer(buffer, fileBytesToRead, fileBytesToRead);

  }

  @Override
  protected int onBufferRead(byte[] b, int off, int len, int cur)
      throws IOException {

    if (fileBytesToRead == 0) {
      return cur == 0 ? -1 : cur;
    }

    if (spilled) {
      buffer.clear();
      feedDataFromFile();
    }
    return cur += read(b, off, len);
  }

}
