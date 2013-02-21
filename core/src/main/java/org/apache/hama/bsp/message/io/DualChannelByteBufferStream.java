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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.Constants;

/**
 * A synchronous i/o stream that is used to write data into and to read back the
 * written data.
 */
public class DualChannelByteBufferStream {

  private DirectByteBufferOutputStream outputBuffer;
  private SyncFlushByteBufferOutputStream outputStream;
  private DirectByteBufferInputStream inputBuffer;
  private SyncReadByteBufferInputStream inputStream;

  private String fileName;

  private ByteBuffer buffer;
  private boolean outputMode;
  private boolean inputMode;

  public void init(Configuration conf) {

    boolean directAlloc = conf.getBoolean(Constants.BYTEBUFFER_DIRECT,
        Constants.BYTEBUFFER_DIRECT_DEFAULT);
    int size = conf.getInt(Constants.BYTEBUFFER_SIZE,
        Constants.BUFFER_DEFAULT_SIZE);
    if (directAlloc) {
      buffer = ByteBuffer.allocateDirect(size);
    } else {
      buffer = ByteBuffer.allocateDirect(size);
    }
    fileName = conf.get(Constants.DATA_SPILL_PATH) + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);
    outputMode = true;
    outputStream = new SyncFlushByteBufferOutputStream(fileName);
    outputBuffer = new DirectByteBufferOutputStream(outputStream);
    outputStream.setBuffer(buffer);

  }

  public DirectByteBufferOutputStream getOutputStream() {
    return outputBuffer;
  }

  public void closeOutput() throws IOException {
    if (outputMode) {
      outputBuffer.close();
    }
    outputMode = false;
  }

  public void close() throws IOException {
    closeInput();
    closeOutput();
  }

  public boolean prepareRead() throws IOException {
    outputStream.close();
    outputMode = false;
    buffer.clear();
    inputStream = new SyncReadByteBufferInputStream(outputStream.isSpilled(),
        fileName);
    inputBuffer = new DirectByteBufferInputStream(inputStream);
    inputBuffer.setBuffer(buffer);
    inputMode = true;
    return true;
  }

  public DirectByteBufferInputStream getInputStream() {
    return inputBuffer;
  }

  public void closeInput() throws IOException {
    if (inputMode) {
      inputBuffer.close();
    }
    inputMode = false;
  }

}
