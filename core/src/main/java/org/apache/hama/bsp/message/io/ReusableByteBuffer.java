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
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * 
 * 
 * @param <M>
 */
public class ReusableByteBuffer<M extends Writable> implements Iterable<M> {

  private DirectByteBufferInputStream stream;
  private SpilledByteBuffer buffer;
  private boolean isIterStarted;

  private M message;

  private static class ReusableByteBufferIterator<M extends Writable>
      implements Iterator<M> {

    private ReusableByteBuffer<M> buffer;
    private M message;

    public ReusableByteBufferIterator(ReusableByteBuffer<M> bbuffer, M msg) {
      this.buffer = bbuffer;
      this.message = msg;
    }

    @Override
    public boolean hasNext() {
      if (!buffer.isIterStarted) {
        throw new IllegalStateException(
            "Iterator should be reinitialized to work with new buffer.");
      }
      return buffer.stream.hasDataToRead();
    }

    @Override
    public M next() {
      if (!buffer.isIterStarted) {
        throw new IllegalStateException(
            "Iterator should be reinitialized to work with new buffer.");
      }
      try {
        message.readFields(this.buffer.stream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return message;
    }

    @Override
    public void remove() {
    }
  }

  public ReusableByteBuffer(M reusableObject) {
    stream = new DirectByteBufferInputStream();
    message = reusableObject;
  }

  public void set(SpilledByteBuffer buffer) throws IOException {
    this.buffer = buffer;
    stream.setBuffer(this.buffer);
    isIterStarted = false;
  }

  public void setReusableObject(M object) {
    this.message = object;
  }

  @Override
  public Iterator<M> iterator() {
    if (isIterStarted) {
      throw new UnsupportedOperationException(
          "Only one iterator creation is allowed.");
    }
    isIterStarted = true;
    return new ReusableByteBufferIterator<M>(this, message);
  }

  public void prepareForNext() throws IOException {
    this.stream.prepareForNext();
  }

}
