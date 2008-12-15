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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BlockPosition implements Writable, Map<Text, IntegerEntry> {

  public BlockID row;
  public BlockPositionMapWritable<Text, IntegerEntry> entries;

  public BlockPosition() {
    this(new BlockPositionMapWritable<Text, IntegerEntry>());
  }

  public BlockPosition(BlockPositionMapWritable<Text, IntegerEntry> entries) {
    this.entries = entries;
  }

  public int getIndex(String key) {
    return this.entries.get(new Text(key)).getValue();
  }
  
  public int size() {
    return this.entries.size();
  }
  
  public IntegerEntry put(Text key, IntegerEntry value) {
    throw new UnsupportedOperationException("VectorWritable is read-only!");
  }

  public IntegerEntry get(Object key) {
    return this.entries.get(key);
  }

  public IntegerEntry remove(Object key) {
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

  public Set<Text> keySet() {
    Set<Text> result = new TreeSet<Text>();
    for (Text w : entries.keySet()) {
      result.add(w);
    }
    return result;
  }

  public Set<Map.Entry<Text, IntegerEntry>> entrySet() {
    return Collections.unmodifiableSet(this.entries.entrySet());
  }

  public Collection<IntegerEntry> values() {
    ArrayList<IntegerEntry> result = new ArrayList<IntegerEntry>();
    for (Writable w : entries.values()) {
      result.add((IntegerEntry) w);
    }
    return result;
  }

  public void readFields(final DataInput in) throws IOException {
    this.row = new BlockID(Bytes.readByteArray(in));
    this.entries.readFields(in);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row.getBytes());
    this.entries.write(out);
  }

  public void putAll(Map<? extends Text, ? extends IntegerEntry> m) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * 
   * The inner class for an entry of row.
   * 
   */
  public static class Entries implements Map.Entry<byte[], IntegerEntry> {

    private final byte[] column;
    private final IntegerEntry entry;

    Entries(byte[] column, IntegerEntry entry) {
      this.column = column;
      this.entry = entry;
    }

    public IntegerEntry setValue(IntegerEntry c) {
      throw new UnsupportedOperationException("VectorWritable is read-only!");
    }

    public byte[] getKey() {
      byte[] key = column;
      return key;
    }

    public IntegerEntry getValue() {
      return entry;
    }
  }

}
