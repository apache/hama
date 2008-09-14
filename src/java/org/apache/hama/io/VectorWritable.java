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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Vector;
import org.apache.hama.util.Numeric;

public abstract class VectorWritable implements Writable,
    Map<Integer, VectorEntry> {

  public Integer row;
  public VectorMapWritable<Integer, VectorEntry> entries;

  public VectorEntry put(Integer key, VectorEntry value) {
    throw new UnsupportedOperationException("VectorWritable is read-only!");
  }

  public VectorEntry get(Object key) {
    return this.entries.get(key);
  }

  public VectorEntry remove(Object key) {
    throw new UnsupportedOperationException("VectorWritable is read-only!");
  }

  public boolean containsKey(Object key) {
    return entries.containsKey(key);
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public void clear() {
    throw new UnsupportedOperationException("VectorDatum is read-only!");
  }

  public Set<Integer> keySet() {
    Set<Integer> result = new TreeSet<Integer>();
    for (Integer w : entries.keySet()) {
      result.add(w);
    }
    return result;
  }

  public Set<Map.Entry<Integer, VectorEntry>> entrySet() {
    return Collections.unmodifiableSet(this.entries.entrySet());
  }

  public Collection<VectorEntry> values() {
    ArrayList<VectorEntry> result = new ArrayList<VectorEntry>();
    for (Writable w : entries.values()) {
      result.add((VectorEntry) w);
    }
    return result;
  }

  public void readFields(final DataInput in) throws IOException {
    this.row = Numeric.bytesToInt(Bytes.readByteArray(in));
    this.entries.readFields(in);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, Numeric.intToBytes(this.row));
    this.entries.write(out);
  }

  public VectorWritable addition(Integer bs, Vector v2) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void putAll(Map<? extends Integer, ? extends VectorEntry> m) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * Get the VectorEntry that corresponds to column
   */
  public VectorEntry get(Integer column) {
    return this.entries.get(column);
  }

  /**
   * Get the VectorEntry that corresponds to column, using a String key
   */
  public VectorEntry get(String key) {
    return get(Bytes.toBytes(key));
  }

  /**
   * Get the double value without timestamp
   */
  public double get(int key) {
    return this.get(Numeric.intToBytes(key)).getValue();
  }

  public int size() {
    return this.entries.size();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("row=");
    sb.append(", cells={");
    boolean moreThanOne = false;
    for (Map.Entry<Integer, VectorEntry> e : this.entries.entrySet()) {
      if (moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append("(column=");
      sb.append(String.valueOf(e.getKey()));
      sb.append(", timestamp=");
      sb.append(Long.toString(e.getValue().getTimestamp()));
      sb.append(", value=");
      byte[] value = Numeric.doubleToBytes(e.getValue().getValue());
      if (Bytes.equals(Numeric.intToBytes(e.getKey()), HConstants.COL_REGIONINFO)) {
        try {
          sb.append(Writables.getHRegionInfo(value).toString());
        } catch (IOException ioe) {
          sb.append(ioe.toString());
        }
      } else {
        sb.append(Bytes.toString(value));
      }
      sb.append(")");
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Returns an Iterator.
   * 
   * @return iterator
   */
  public Iterator<VectorEntry> iterator() {
    return entries.values().iterator();
  }

  /**
   * 
   * The inner class for an entry of row.
   * 
   */
  public static class Entries implements Map.Entry<byte[], VectorEntry> {

    private final byte[] column;
    private final VectorEntry entry;

    Entries(byte[] column, VectorEntry entry) {
      this.column = column;
      this.entry = entry;
    }

    public VectorEntry setValue(VectorEntry c) {
      throw new UnsupportedOperationException("VectorWritable is read-only!");
    }

    public byte[] getKey() {
      byte[] key = column;
      return key;
    }

    public VectorEntry getValue() {
      return entry;
    }
  }
}
