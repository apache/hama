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
package org.apache.hama;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * A feature vector. Features are dimension-value pairs. This class implements a
 * simple dictionary data structure to map dimensions onto their values. Note
 * that for convenience, features do not have be sorted according to their
 * dimensions at this point. The SVMLightTrainer class has an option for sorting
 * input vectors prior to training.
 */
public class FeatureVector {
  static final Logger LOG = Logger.getLogger(FeatureVector.class);
  protected int[] m_dims;
  protected double[] m_vals;
  protected SortedMap<Integer, Double> vector;

  public FeatureVector(RowResult r) {
    SortedMap<Integer, Double> m = new TreeMap<Integer, Double>();

    for (Map.Entry<byte[], Cell> f : r.entrySet()) {
      m.put(getColumnIndex(f.getKey()), Double.parseDouble(Bytes.toString(f.getValue().getValue())));
    }
    parse(m);
  }

  public FeatureVector(SortedMap<Integer, Double> m) {
    parse(m);
  }

  public void parse(SortedMap<Integer, Double> m) {
    this.m_dims = new int[m.keySet().size()];
    this.m_vals = new double[m.keySet().size()];

    int i = 0;
    for (Map.Entry<Integer, Double> f : m.entrySet()) {
      this.m_dims[i] = f.getKey();
      this.m_vals[i] = f.getValue();
      i++;
    }
  }

  /**
   * Returns the cosine similarity between two feature vectors.
   */
  public double getCosine(FeatureVector v) {
    double cosine = 0.0;
    int dim;
    double q_i, d_i;
    for (int i = 0; i < Math.min(this.size(), v.size()); i++) {
      dim = v.getDimAt(i);
      q_i = v.getValueAt(dim);
      d_i = this.getValueAt(dim);
      cosine += q_i * d_i;
    }
    return cosine / (this.getL2Norm() * v.getL2Norm());
  }

  /**
   * Returns the linear norm factor of this vector's values (i.e., the sum of
   * it's values).
   */
  public double getL1Norm() {
    double sum = 0.0;
    for (int i = 0; i < m_vals.length; i++) {
      sum += m_vals[i];
    }
    return sum;
  }

  /**
   * Returns the L2 norm factor of this vector's values.
   */
  public double getL2Norm() {
    double square_sum = 0.0;
    for (int i = 0; i < m_vals.length; i++) {
      square_sum += (m_vals[i] * m_vals[i]);
    }
    return Math.sqrt(square_sum);
  }

  public int getDimAt(int index) {
    return m_dims[index];
  }

  public double getValueAt(int index) {
    return m_vals[index];
  }

  public int size() {
    return m_dims.length;
  }

  public FeatureVector addition(FeatureVector v2) {
    SortedMap<Integer, Double> v3 = new TreeMap<Integer, Double>();
    if (this.size() == v2.size()) {
      for (int i = 0; i < this.size(); i++) {
        LOG.info("Addition: " + this.getValueAt(i) + ", " + v2.getValueAt(i));
        double value = (this.getValueAt(i) + v2.getValueAt(i));
        v3.put(i, value);
      }

      return new FeatureVector(v3);
    } else {
      return null;
    }
  }

  /**
   * Return the integer column index
   * 
   * @param b key
   * @return integer
   */
  public int getColumnIndex(byte[] b) {
    String cKey = new String(b);
    return Integer.parseInt(cKey
        .substring(cKey.indexOf(":") + 1, cKey.length()));
  }

  /**
   * Converts the bytes to double
   * 
   * @param inBytes
   * @return double
   */
  public double toDouble(byte[] inBytes) {
    if (inBytes == null) {
      return 0;
    }

    long n = 0;
    for (int i = 0; i < inBytes.length; i++) {
      n |= ((long) (inBytes[i] & 0377) << (i * 8));
    }

    double doubleValue = Double.longBitsToDouble(n);

    return doubleValue;
  }

  /**
   * Converts the int to byte array
   * 
   * @param i
   * @return Byte Array
   */
  public byte[] intToBytes(int i) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.order(ByteOrder.nativeOrder());
    bb.putInt(i);
    return bb.array();
  }

  public RowResult getRowResult(byte[] row) {
    HbaseMapWritable<byte[], Cell> trunk = new HbaseMapWritable<byte[], Cell>();
    for (int i = 0; i < this.size(); i++) {
      Cell cValue = new Cell(String.valueOf(this.getValueAt(i)), 0);
      trunk.put(Bytes.toBytes("column:" + i), cValue);
    }

    return new RowResult(row, trunk);
  }
}
