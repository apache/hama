/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

public class DoubleEntry implements Writable, Iterable<DoubleEntry> {
  static final Logger LOG = Logger.getLogger(DoubleEntry.class);
  protected byte[][] values;
  protected long[] timestamps;
  
  /** For Writable compatibility */
  public DoubleEntry() {
    values = null;
    timestamps = null;
  }

  public DoubleEntry(Cell c) {
    this.values = new byte[1][];
    this.values[0] = c.getValue();
    this.timestamps = new long[1];
    this.timestamps[0] = c.getTimestamp();
  }

  public DoubleEntry(double value) {
    this.values = new byte[1][];
    this.values[0] = BytesUtil.doubleToBytes(value);
    this.timestamps = new long[1];
    this.timestamps[0] = System.currentTimeMillis();
  }
  
  public DoubleEntry(byte[] value) {
    this.values = new byte[1][];
    this.values[0] = value;
    this.timestamps = new long[1];
    this.timestamps[0] = System.currentTimeMillis();
  }
  
  /** @return the current VectorEntry's value */
  public double getValue() {
    return BytesUtil.bytesToDouble(this.values[0]);
  }

  /** @return the current VectorEntry's timestamp */
  public long getTimestamp() {
    return timestamps[0];
  }

  /**
   * Create a new VectorEntry with a given value and timestamp. Used by HStore.
   * 
   * @param value
   * @param timestamp
   */
  public DoubleEntry(byte[] value, long timestamp) {
    this.values = new byte[1][];
    this.values[0] = value;
    this.timestamps = new long[1];
    this.timestamps[0] = timestamp;
  }

  //
  // Writable
  //

  /** {@inheritDoc} */
  public void readFields(final DataInput in) throws IOException {
    int nvalues = in.readInt();
    this.timestamps = new long[nvalues];
    this.values = new byte[nvalues][];
    for (int i = 0; i < nvalues; i++) {
      this.timestamps[i] = in.readLong();
    }
    for (int i = 0; i < nvalues; i++) {
      this.values[i] = Bytes.readByteArray(in);
    }
  }

  /** {@inheritDoc} */
  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.values.length);
    for (int i = 0; i < this.timestamps.length; i++) {
      out.writeLong(this.timestamps[i]);
    }
    for (int i = 0; i < this.values.length; i++) {
      Bytes.writeByteArray(out, this.values[i]);
    }
  }

  //
  // Iterable
  //

  /** {@inheritDoc} */
  public Iterator<DoubleEntry> iterator() {
    return new VectorEntryIterator();
  }

  private class VectorEntryIterator implements Iterator<DoubleEntry> {
    private int currentValue = -1;

    VectorEntryIterator() {
    }

    /** {@inheritDoc} */
    public boolean hasNext() {
      return currentValue < values.length;
    }

    /** {@inheritDoc} */
    public DoubleEntry next() {
      currentValue += 1;
      return new DoubleEntry(values[currentValue], timestamps[currentValue]);
    }

    /** {@inheritDoc} */
    public void remove() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("remove is not supported");
    }
  }
}
