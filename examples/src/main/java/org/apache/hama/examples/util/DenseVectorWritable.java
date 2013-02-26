/**
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
package org.apache.hama.examples.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class represents dense vector. It will improve memory consumption up to
 * two times in comparison to SparseVectorWritable in case of vectors which
 * sparsity is close to 1. Internally represents vector values as array. Can be
 * used in SpMV for representation of input and output vector.
 */
public class DenseVectorWritable implements Writable {

  private double values[];

  public DenseVectorWritable() {
    values = new double[0];
  }

  public int getSize() {
    return values.length;
  }

  public void setSize(int size) {
    values = new double[size];
  }

  public double get(int index) {
    return values[index];
  }

  public void addCell(int index, double value) {
    values[index] = value;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    int len = in.readInt();
    setSize(size);
    for (int i = 0; i < len; i++) {
      int index = in.readInt();
      double value = in.readDouble();
      values[index] = value;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(getSize());
    out.writeInt(getSize());
    for (int i = 0; i < getSize(); i++) {
      out.writeInt(i);
      out.writeDouble(values[i]);
    }
  }

  @Override
  public String toString() {
    StringBuilder st = new StringBuilder();
    st.append(" " + getSize() + " " + getSize());
    for (int i = 0; i < getSize(); i++)
      st.append(" " + i + " " + values[i]);
    return st.toString();
  }

}
