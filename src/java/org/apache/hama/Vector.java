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

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.io.VectorDatum;
import org.apache.log4j.Logger;

public class Vector extends AbstractBase implements VectorInterface {
  static final Logger LOG = Logger.getLogger(Vector.class);
  protected int[] m_dims;
  protected double[] m_vals;

  public Vector(RowResult r) {
    parse(r.entrySet());
  }

  public Vector(VectorDatum r) {
    parse(r.entrySet());
  }

  private void parse(Set<Entry<byte[], Cell>> entrySet) {
    SortedMap<Integer, Double> m = new TreeMap<Integer, Double>();
    for (Map.Entry<byte[], Cell> f : entrySet) {
      m.put(getColumnIndex(f.getKey()), Double.parseDouble(Bytes.toString(f
          .getValue().getValue())));
    }

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
  public double getCosine(Vector v) {
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

  public VectorDatum addition(byte[] bs, Vector v2) {
    HbaseMapWritable<byte[], Cell> trunk = new HbaseMapWritable<byte[], Cell>();
    for (int i = 0; i < this.size(); i++) {
      double value = (this.getValueAt(i) + v2.getValueAt(i));
      Cell cValue = new Cell(String.valueOf(value), 0);
      trunk.put(Bytes.toBytes("column:" + i), cValue);
    }

    return new VectorDatum(bs, trunk);
  }
}
