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
package org.apache.hama.matrix;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

/**
 * Methods of the vector classes
 */
public abstract class AbstractVector {
  static final Logger LOG = Logger.getLogger(AbstractVector.class);
  protected MapWritable entries;

  public void initMap(Result rs) {
    this.entries = new MapWritable();
    NavigableMap<byte[], byte[]> map = rs.getFamilyMap(Constants.COLUMNFAMILY);
    for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
      if(e != null) {
        this.entries.put(new IntWritable(BytesUtil.bytesToInt(e.getKey())),
            new DoubleWritable(BytesUtil.bytesToDouble(e.getValue())));
      }
    }
  }
  
  /**
   * Returns an Iterator.
   * 
   * @return iterator
   */
  public Iterator<Writable> iterator() {
    return this.entries.values().iterator();
  }

  /**
   * Returns a size of vector. If vector is sparse, returns the number of only
   * non-zero elements.
   * 
   * @return a size of vector
   */
  public int size() {
    int x = 0;
    if (this.entries != null && this.entries.containsKey(new Text("row")))
      x = 1;

    return (this.entries != null) ? this.entries.size() - x : 0;
  }

  /**
   * Returns the {@link org.apache.hadoop.io.MapWritable}
   * 
   * @return the entries of vector
   */
  public MapWritable getEntries() {
    return this.entries;
  }

  /**
   * Checks for conformant sizes
   */
  protected void checkComformantSize(Vector v2) {
    if (this.size() != v2.size()) {
      throw new IndexOutOfBoundsException("v1.size != v2.size (" + this.size()
          + " != " + v2.size() + ")");
    }
  }

  /**
   * Clears the entries.
   */
  public void clear() {
    this.entries = null;
  }
}
