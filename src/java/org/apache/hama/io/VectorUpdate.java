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

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.util.BytesUtil;

public class VectorUpdate {
  private Put put;

  public VectorUpdate(int i) {
    this.put = new Put(BytesUtil.getRowIndex(i));
  }

  public VectorUpdate(String row) {
    this.put = new Put(Bytes.toBytes(row));
  }

  public VectorUpdate(byte[] row) {
    this.put = new Put(row);
  }

  public void put(int j, double value) {
    this.put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j)),
        BytesUtil.doubleToBytes(value));
  }

  public void put(String cfName, int j, double value) {
    this.put.add(Bytes.toBytes(cfName), Bytes.toBytes(String.valueOf(j)), Bytes.toBytes(value));
  }

  public void put(String column, String qualifier, String val) {
    this.put.add(Bytes.toBytes(column), Bytes.toBytes(qualifier), Bytes
        .toBytes(val));
  }

  public void put(String column, String qualifier, double val) {
    this.put.add(Bytes.toBytes(column), Bytes.toBytes(qualifier), Bytes
        .toBytes(val));
  }

  public void putAll(Map<Integer, Double> buffer) {
    for (Map.Entry<Integer, Double> f : buffer.entrySet()) {
      put(f.getKey(), f.getValue());
    }
  }

  public void putAll(Set<Entry<Integer, DoubleEntry>> entrySet) {
    for (Map.Entry<Integer, DoubleEntry> e : entrySet) {
      put(e.getKey(), e.getValue().getValue());
    }
  }

  public void putAll(MapWritable entries) {
    for (Map.Entry<Writable, Writable> e : entries.entrySet()) {
      put(((IntWritable) e.getKey()).get(), ((DoubleEntry) e.getValue())
          .getValue());
    }
  }

  public Put getPut() {
    return this.put;
  }
}
