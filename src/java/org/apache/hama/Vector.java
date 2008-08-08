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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.io.VectorWritable;
import org.apache.log4j.Logger;

public class Vector extends VectorWritable implements VectorInterface {
  static final Logger LOG = Logger.getLogger(Vector.class);

  public Vector() {
    this(null, new HbaseMapWritable<byte[], Cell>());
  }

  public Vector(final byte[] row, final HbaseMapWritable<byte[], Cell> m) {
    this.row = row;
    this.cells = m;
  }

  /**
   * @param row
   * @param matrix
   */
  public Vector(byte[] row, Matrix matrix) {
    this.row = row;
    parse(matrix.getRowResult(this.row).entrySet());
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
    return cosine / (this.getNorm2() * v.getNorm2());
  }

  public double get(int index) {
    return bytesToInt(this.cells.get(intToBytes(index)).getValue());
  }

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

  public void set(int index, double value) {
    // TODO Auto-generated method stub

  }

  public Vector set(Vector v) {
    // TODO Auto-generated method stub
    return null;
  }

  public double getNorm1() {
    double sum = 0.0;
    for (int i = 0; i < m_vals.length; i++) {
      sum += m_vals[i];
    }
    return sum;
  }

  public double getNorm2() {
    double square_sum = 0.0;
    for (int i = 0; i < m_vals.length; i++) {
      square_sum += (m_vals[i] * m_vals[i]);
    }
    return Math.sqrt(square_sum);
  }

  public double getNorm2Robust() {
    // TODO Auto-generated method stub
    return 0;
  }

  public double getNormInf() {
    // TODO Auto-generated method stub
    return 0;
  }

  public int getDimAt(int index) {
    return m_dims[index];
  }

  public double getValueAt(int index) {
    return m_vals[index];
  }
}
