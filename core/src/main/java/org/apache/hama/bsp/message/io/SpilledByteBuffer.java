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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import org.apache.hadoop.io.Writable;

/**
 * <code>SpilledByteBuffer</code> encapsulates a ByteBuffer. It lets the user to
 * mark the end of last record written into the buffer.
 * 
 */
public class SpilledByteBuffer {

  ByteBuffer buffer;
  int endOfRecord;
  Class<? extends Writable> writableClass;

  public SpilledByteBuffer(boolean direct, int size) {
    if (direct) {
      buffer = ByteBuffer.allocate(size);
    } else {
      buffer = ByteBuffer.allocateDirect(size);
    }
  }

  public void setRecordClass(Class<? extends Writable> classObj) {
    this.writableClass = classObj;
  }

  public Class<? extends Writable> getRecordClass() {
    return this.writableClass;
  }

  public SpilledByteBuffer(ByteBuffer byteBuffer) {
    this.buffer = byteBuffer;
  }

  public SpilledByteBuffer(ByteBuffer byteBuffer, int markEnd) {
    this.buffer = byteBuffer;
    this.endOfRecord = markEnd;
  }

  public void markEndOfRecord() {
    this.endOfRecord = this.buffer.position();
  }

  public void markEndOfRecord(int pos) {
    if (pos < this.buffer.capacity()) {
      this.endOfRecord = pos;
    }
  }

  public int getMarkofLastRecord() {
    return this.endOfRecord;
  }

  public ByteBuffer getByteBuffer() {
    return buffer;
  }

  public CharBuffer asCharBuffer() {
    return buffer.asCharBuffer();
  }

  public DoubleBuffer asDoubleBuffer() {
    return buffer.asDoubleBuffer();
  }

  public FloatBuffer asFloatBuffer() {
    return buffer.asFloatBuffer();
  }

  public IntBuffer asIntBuffer() {
    return buffer.asIntBuffer();
  }

  public LongBuffer asLongBuffer() {
    return buffer.asLongBuffer();
  }

  public SpilledByteBuffer asReadOnlyBuffer() {
    return new SpilledByteBuffer(buffer.asReadOnlyBuffer());
  }

  public ShortBuffer asShortBuffer() {
    return buffer.asShortBuffer();
  }

  public SpilledByteBuffer compact() {
    buffer.compact();
    return this;
  }

  public SpilledByteBuffer duplicate() {
    buffer.duplicate();
    return new SpilledByteBuffer(this.buffer, this.endOfRecord);
  }

  public byte get() {
    return buffer.get();
  }

  public byte get(int index) {
    return buffer.get(index);
  }

  public char getChar() {
    return buffer.getChar();
  }

  public char getChar(int index) {
    return buffer.getChar(index);
  }

  public double getDouble() {
    return buffer.getDouble();
  }

  public double getDouble(int index) {
    return buffer.getDouble(index);
  }

  public float getFloat() {
    return buffer.getFloat();
  }

  public float getFloat(int index) {
    return buffer.getFloat(index);
  }

  public int getInt() {
    return buffer.getInt();
  }

  public int getInt(int index) {
    return buffer.getInt(index);
  }

  public long getLong() {
    return buffer.getLong();
  }

  public long getLong(int index) {
    return buffer.getLong();
  }

  public short getShort() {
    return buffer.getShort();
  }

  public short getShort(int index) {
    return buffer.getShort(index);
  }

  public SpilledByteBuffer put(byte b) {
    buffer.put(b);
    return this;
  }

  public SpilledByteBuffer put(int index, byte b) {
    buffer.put(index, b);
    return this;
  }

  public SpilledByteBuffer putChar(char value) {
    buffer.putChar(value);
    return this;
  }

  public SpilledByteBuffer putChar(int index, char value) {
    buffer.putChar(index, value);
    return this;
  }

  public SpilledByteBuffer putDouble(double value) {
    buffer.putDouble(value);
    return this;
  }

  public SpilledByteBuffer putDouble(int index, double value) {
    buffer.putDouble(index, value);
    return this;
  }

  public SpilledByteBuffer putFloat(float value) {
    buffer.putFloat(value);
    return this;
  }

  public SpilledByteBuffer putFloat(int index, float value) {
    buffer.putFloat(index, value);
    return this;
  }

  public SpilledByteBuffer putInt(int index, int value) {
    buffer.putInt(index, value);
    return this;
  }

  public SpilledByteBuffer putInt(int value) {
    buffer.putInt(value);
    return this;
  }

  public SpilledByteBuffer putLong(int index, long value) {
    buffer.putLong(index, value);
    return this;
  }

  public SpilledByteBuffer putLong(long value) {
    buffer.putLong(value);
    return this;
  }

  public SpilledByteBuffer putShort(int index, short value) {
    buffer.putShort(index, value);
    return this;
  }

  public SpilledByteBuffer putShort(short value) {
    buffer.putShort(value);
    return this;
  }

  public SpilledByteBuffer slice() {
    return new SpilledByteBuffer(buffer.slice());
  }

  public byte[] array() {
    return buffer.array();
  }

  public int arrayOffset() {
    return buffer.arrayOffset();
  }

  public boolean hasArray() {
    return buffer.hasArray();
  }

  public boolean isDirect() {
    return buffer.isDirect();
  }

  public boolean isReadOnly() {
    return buffer.isReadOnly();
  }

  public void clear() {
    buffer.clear();
  }

  public SpilledByteBuffer flip() {
    buffer.flip();
    return this;
  }

  public int remaining() {
    return buffer.remaining();
  }

  public void put(byte[] b, int off, int rem) {
    buffer.put(b, off, rem);
  }

  public void put(ByteBuffer byteBuffer) {
    buffer.put(byteBuffer);

  }

  public int capacity() {
    return this.buffer.capacity();
  }

  public void get(byte[] b, int off, int readSize) {
    buffer.get(b, off, readSize);

  }
}
