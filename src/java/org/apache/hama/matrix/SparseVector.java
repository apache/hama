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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.matrix.Vector.Norm;
import org.apache.log4j.Logger;

/**
 * This class represents a sparse vector.
 */
public class SparseVector extends AbstractVector implements Vector {
  static final Logger LOG = Logger.getLogger(SparseVector.class);

  public SparseVector() {
    this(new MapWritable());
  }

  public SparseVector(MapWritable m) {
    this.entries = m;
  }

  public SparseVector(RowResult row) {
    this.initMap(row);
  }

  @Override
  public Vector add(double alpha, Vector v) {
    if (alpha == 0)
      return this;

    for (Map.Entry<Writable, Writable> e : v.getEntries().entrySet()) {
      if (this.entries.containsKey(e.getKey())) {
        // add
        double value = alpha * ((DoubleEntry) e.getValue()).getValue()
            + this.get(((IntWritable) e.getKey()).get());
        this.entries.put(e.getKey(), new DoubleEntry(value));
      } else {
        // put
        double value = alpha * ((DoubleEntry) e.getValue()).getValue();
        this.entries.put(e.getKey(), new DoubleEntry(value));
      }
    }

    return this;
  }

  /**
   * x = v + x
   * 
   * @param v2
   * @return x = v + x
   */
  public SparseVector add(Vector v2) {

    for (Map.Entry<Writable, Writable> e : v2.getEntries().entrySet()) {
      int key = ((IntWritable) e.getKey()).get();
      if (this.entries.containsKey(e.getKey())) {
        this.add(key, ((DoubleEntry) e.getValue()).getValue());
      } else {
        this.set(key, ((DoubleEntry) e.getValue()).getValue());
      }
    }

    return this;
  }

  @Override
  public double dot(Vector v) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double norm(Norm type) {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * v = alpha*v
   * 
   * @param alpha
   * @return v = alpha*v
   */
  public SparseVector scale(double alpha) {
    for (Map.Entry<Writable, Writable> e : this.entries.entrySet()) {
      this.entries.put(e.getKey(), new DoubleEntry(((DoubleEntry) e.getValue())
          .getValue()
          * alpha));
    }
    return this;
  }

  /**
   * Gets the value of index
   * 
   * @param index
   * @return the value of v(index)
   * @throws IOException
   */
  public double get(int index) {
    double value;
    try {
      value = ((DoubleEntry) this.entries.get(new IntWritable(index)))
          .getValue();
    } catch (NullPointerException e) { // returns zero if there is no value
      return 0;
    }

    return value;
  }

  /**
   * Sets the value of index
   * 
   * @param index
   * @param value
   */
  public void set(int index, double value) {
    // If entries are null, create new object
    if (this.entries == null) {
      this.entries = new MapWritable();
    }

    if (value != 0) // only stores non-zero element
      this.entries.put(new IntWritable(index), new DoubleEntry(value));
  }

  /**
   * Adds the value to v(index)
   * 
   * @param index
   * @param value
   */
  public void add(int index, double value) {
    set(index, get(index) + value);
  }

  /**
   * Sets the vector
   * 
   * @param v
   * @return x = v
   */
  public SparseVector set(Vector v) {
    return new SparseVector(v.getEntries());
  }

  @Override
  public Vector subVector(int i0, int i1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Vector set(double alpha, Vector v) {
    // TODO Auto-generated method stub
    return null;
  }

}
