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

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.io.VectorWritable;
import org.apache.log4j.Logger;

public class Vector extends VectorWritable implements VectorInterface {
  static final Logger LOG = Logger.getLogger(Vector.class);

  public enum Norm {
    // TODO : Add types
  }

  public Vector() {
    this(null, new HbaseMapWritable<byte[], Cell>());
  }

  /**
   * Create a RowResult from a row and Cell map
   */
  public Vector(final byte[] row, final HbaseMapWritable<byte[], Cell> m) {
    this.row = row;
    this.cells = m;
  }

  public byte[] getRow() {
    return row;
  }

  public Vector(RowResult r) {
    parse(r.entrySet());
  }

  public Vector(VectorWritable r) {
    parse(r.entrySet());
  }

  public Vector(byte[] rowKey, Matrix b) {
    this.row = rowKey;
    parse(b.getRowResult(this.row).entrySet());
  }

  public void add(int index, double value) {
    // TODO Auto-generated method stub

  }

  public boolean add(double alpha, Vector v) {
    // TODO Auto-generated method stub
    return false;
  }

  public Vector add(Vector v2) {
    HbaseMapWritable<byte[], Cell> trunk = new HbaseMapWritable<byte[], Cell>();
    for (int i = 0; i < this.size(); i++) {
      double value = (this.getValueAt(i) + v2.getValueAt(i));
      Cell cValue = new Cell(String.valueOf(value), 0);
      trunk.put(Bytes.toBytes("column:" + i), cValue);
    }

    return new Vector(row, trunk);
  }

  public double dot(Vector v) {
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

  public double get(int index) {
    // TODO Auto-generated method stub
    return 0;
  }

  public double norm(Norm type) {
    // TODO Auto-generated method stub
    return 0;
  }

  public void set(int index, double value) {
    // TODO Auto-generated method stub

  }

  public Vector set(Vector v) {
    // TODO Auto-generated method stub
    return null;
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
}
