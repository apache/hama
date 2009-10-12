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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.io.DoubleEntry;
import org.apache.log4j.Logger;

/**
 * This class represents a dense vector.
 */
public class DenseVector extends AbstractVector implements Vector {
  static final Logger LOG = Logger.getLogger(DenseVector.class);

  public DenseVector() {
    this(new MapWritable());
  }

  public DenseVector(RowResult row) {
    this.initMap(row);
  }
  
  public DenseVector(MapWritable m) {
    this.entries = m;
  }

  public DenseVector(Result row) {
    this.initMap(row);
  }

  public DenseVector(int row, MapWritable m) {
    this.entries = m;
    this.entries.put(Constants.ROWCOUNT, new IntWritable(row));
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

    this.entries.put(new IntWritable(index), new DoubleEntry(value));
  }

  /**
   * Sets the vector
   * 
   * @param v
   * @return x = v
   */
  public DenseVector set(Vector v) {
    this.set(1, v);
    return this;
  }

  public Vector set(double alpha, Vector v) {
    checkComformantSize(v);
    boolean zeroFill = false;
    if(alpha == 0) 
      zeroFill = true;
    
    for (Map.Entry<Writable, Writable> e : v.getEntries().entrySet()) {
      int key = ((IntWritable) e.getKey()).get();
      if(zeroFill)
        this.set(key, 0);
      else
        this.set(key, alpha * ((DoubleEntry) e.getValue()).getValue());
    }
    
    return this;
  }
  
  public void setRow(int row) {
    this.entries.put(Constants.ROWCOUNT, new IntWritable(row));
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
    } catch (NullPointerException e) {
      throw new NullPointerException("Unexpected null value : " + e.toString());
    }

    return value;
  }
  
  public int getRow() {
    return ((IntWritable) this.entries.get(Constants.ROWCOUNT)).get();
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
   * x = alpha*v + x
   * 
   * @param alpha
   * @param v
   * @return x = alpha*v + x
   */
  public DenseVector add(double alpha, Vector v) {
    checkComformantSize(v);
    if (alpha == 0)
      return this;

    for (Map.Entry<Writable, Writable> e : this.getEntries().entrySet()) {
      int key = ((IntWritable) e.getKey()).get();
      this.add(key, alpha * v.get(key));
    }

    return this;
  }

  /**
   * x = v + x
   * 
   * @param v2
   * @return x = v + x
   */
  public DenseVector add(Vector v2) {
    checkComformantSize(v2);

    for (Map.Entry<Writable, Writable> e : this.getEntries().entrySet()) {
      int key = ((IntWritable) e.getKey()).get();
      this.add(key, v2.get(key));
    }

    return this;
  }

  /**
   * x dot v
   * 
   * @param v
   * @return x dot v
   */
  public double dot(Vector v) {
    checkComformantSize(v);
    
    double cosine = 0.0;
    double q_i, d_i;
    for (int i = 0; i < Math.min(this.size(), v.size()); i++) {
      q_i = v.get(i);
      d_i = this.get(i);
      cosine += q_i * d_i;
    }
    return cosine / (this.getNorm2() * ((DenseVector) v).getNorm2());
  }

  /**
   * v = alpha*v
   * 
   * @param alpha
   * @return v = alpha*v
   */
  public DenseVector scale(double alpha) {
    for (Map.Entry<Writable, Writable> e : this.entries.entrySet()) {
      this.entries.put(e.getKey(), new DoubleEntry(((DoubleEntry) e.getValue())
          .getValue()
          * alpha));
    }
    return this;
  }

  /**
   * Computes the given norm of the vector
   * 
   * @param type
   * @return norm of the vector
   */
  public double norm(Norm type) {
    if (type == Norm.One)
      return getNorm1();
    else if (type == Norm.Two)
      return getNorm2();
    else if (type == Norm.TwoRobust)
      return getNorm2Robust();
    else
      return getNormInf();
  }

  protected double getNorm1() {
    double sum = 0.0;

    Set<Writable> keySet = this.entries.keySet();
    Iterator<Writable> it = keySet.iterator();

    while (it.hasNext()) {
      sum += get(((IntWritable) it.next()).get());
    }

    return sum;
  }

  protected double getNorm2() {
    double square_sum = 0.0;

    Set<Writable> keySet = entries.keySet();
    Iterator<Writable> it = keySet.iterator();

    while (it.hasNext()) {
      double value = get(((IntWritable) it.next()).get());
      square_sum += value * value;
    }

    return Math.sqrt(square_sum);
  }

  /**
   * Returns the robust norm of the vector
   * 
   * @return the robust norm of the vector
   */
  protected double getNorm2Robust() {
    double scale = 0, ssq = 1;
    for (int i = 0; i < this.size(); i++) {
      double val = get(i);
      if (val != 0) {
        double absxi = Math.abs(val);
        if (scale < absxi) {
          ssq = 1 + ssq * Math.pow(scale / absxi, 2);
          scale = absxi;
        } else
          ssq = ssq + Math.pow(absxi / scale, 2);
      }
    }
    return scale * Math.sqrt(ssq);
  }

  /**
   * Returns the infinity norm of the vector
   * 
   * @return the infinity norm of the vector
   */
  protected double getNormInf() {
    double max = 0.0;
    for (int i = 0; i < this.size(); i++) {
      max = Math.max(max, Math.abs(get(i)));
    }
    return max;
  }

  /**
   * Returns a sub-vector.
   * 
   * @param i0 the index of the first element
   * @param i1 the index of the last element
   * @return v[i0:i1]
   */
  public DenseVector subVector(int i0, int i1) {
    DenseVector res = new DenseVector();
    if (this.entries.containsKey(Constants.ROWCOUNT))
      res.setRow(this.getRow());

    for (int i = i0; i <= i1; i++) {
      res.set(i, get(i));
    }

    return res;
  }

  public void zeroFill(int size) {
    for(int i = 0; i < size; i++) {
      this.set(i, 0);
    }
  }
}
