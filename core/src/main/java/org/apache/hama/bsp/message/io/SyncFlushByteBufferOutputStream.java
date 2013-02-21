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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A {@link ByteBuffer} stream that synchronously writes the spilled data to
 * local storage.
 * 
 */
public class SyncFlushByteBufferOutputStream extends ByteBufferOutputStream {

  String fileName;
  FileChannel channel;
  FileOutputStream stream;
  private boolean spilled;

  public SyncFlushByteBufferOutputStream(String fileName) {
    super();
    this.fileName = fileName;
  }

  @Override
  protected boolean onBufferFull(byte[] b, int off, int len) throws IOException {
    buffer.flip();
    if (channel == null) {
      File f = new File(fileName);
      stream = new FileOutputStream(f, true);
      channel = stream.getChannel();
    }
    channel.write(buffer);
    channel.write(ByteBuffer.wrap(b, off, len));

    channel.force(true);
    buffer.clear();
    spilled = true;
    return false;
  }

  @Override
  protected void onFlush() throws IOException {
    if (spilled) {
      buffer.flip();
      channel.write(buffer);
      channel.force(true);
      channel.close();
    }
  }

  public boolean isSpilled() {
    return spilled;
  }

}
