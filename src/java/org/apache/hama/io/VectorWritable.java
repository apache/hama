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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hama.DenseVector;
import org.apache.hama.Vector;
import org.apache.hama.util.BytesUtil;

public class VectorWritable implements Writable, Map<Integer, DoubleEntry> {

  public Integer row;
  public MapWritable<Integer, DoubleEntry> entries;

  public VectorWritable() {
    this(new MapWritable<Integer, DoubleEntry>());
  }

  public VectorWritable(MapWritable<Integer, DoubleEntry> entries) {
    this.entries = entries;
  }

  public VectorWritable(int row, DenseVector v) {
    this.row = row;
    this.entries = v.entries;
  }

  public DenseVector getDenseVector() {
    return new DenseVector(entries);
  }

  public Double get(Integer column) {
    return this.entries.get(column).getValue();
  }

  public int size() {
    return this.entries.size();
  }
  
  public DoubleEntry put(Integer key, DoubleEntry value) {
    throw new UnsupportedOperationException("VectorWritable is read-only!");
  }

  public DoubleEntry get(Object key) {
    return this.entries.get(key);
  }

  public DoubleEntry remove(Object key) {
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

  public Set<Map.Entry<Integer, DoubleEntry>> entrySet() {
    return Collections.unmodifiableSet(this.entries.entrySet());
  }

  public Collection<DoubleEntry> values() {
    ArrayList<DoubleEntry> result = new ArrayList<DoubleEntry>();
    for (Writable w : entries.values()) {
      result.add((DoubleEntry) w);
    }
    return result;
  }

  public void readFields(final DataInput in) throws IOException {
    this.row = BytesUtil.bytesToInt(Bytes.readByteArray(in));
    this.entries.readFields(in);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, BytesUtil.intToBytes(this.row));
    this.entries.write(out);
  }

  public VectorWritable addition(Integer bs, Vector v2) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void putAll(Map<? extends Integer, ? extends DoubleEntry> m) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * 
   * The inner class for an entry of row.
   * 
   */
  public static class Entries implements Map.Entry<byte[], DoubleEntry> {

    private final byte[] column;
    private final DoubleEntry entry;

    Entries(byte[] column, DoubleEntry entry) {
      this.column = column;
      this.entry = entry;
    }

    public DoubleEntry setValue(DoubleEntry c) {
      throw new UnsupportedOperationException("VectorWritable is read-only!");
    }

    public byte[] getKey() {
      byte[] key = column;
      return key;
    }

    public DoubleEntry getValue() {
      return entry;
    }
  }
}
