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
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Encapsulates {@link ByteBufferInputStream}
 */
public class DirectByteBufferInputStream extends DataInputStream implements
    DataInput {

  public DirectByteBufferInputStream(ByteBufferInputStream in) {
    super(in);
  }

  public DirectByteBufferInputStream() {
    super(new ByteBufferInputStream());
  }

  public void prepareForNext() throws IOException {
    ByteBufferInputStream stream = (ByteBufferInputStream) this.in;
    stream.fillForNext();
  }

  public boolean hasDataToRead() {
    ByteBufferInputStream stream = (ByteBufferInputStream) this.in;
    return stream.hasDataToRead();
  }

  public boolean hasUnmarkData() {
    ByteBufferInputStream stream = (ByteBufferInputStream) this.in;
    return stream.hasUnmarkedData();
  }

  public void setBuffer(SpilledByteBuffer buff) throws IOException {
    ByteBufferInputStream stream = (ByteBufferInputStream) this.in;
    stream.setBuffer(buff.getByteBuffer(), buff.getMarkofLastRecord(),
        buff.remaining());
  }

  public void setBuffer(ByteBuffer buffer) throws IOException {
    ByteBufferInputStream stream = (ByteBufferInputStream) this.in;
    stream.setBuffer(buffer, buffer.remaining(), buffer.remaining());
  }

}
