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
package org.apache.hama.ml.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.ml.math.DenseDoubleMatrix;
import org.apache.hama.ml.math.DoubleMatrix;

/**
 * Majorly designed for dense matrices, can be extended for sparse ones as well.
 */
public final class MatrixWritable implements Writable {

  private DoubleMatrix mat;

  public MatrixWritable() {
  }

  public MatrixWritable(DoubleMatrix mat) {
    this.mat = mat;

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    mat = read(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    write(mat, out);
  }

  public static void write(DoubleMatrix mat, DataOutput out) throws IOException {
    out.writeInt(mat.getRowCount());
    out.writeInt(mat.getColumnCount());
    for (int row = 0; row < mat.getRowCount(); row++) {
      for (int col = 0; col < mat.getColumnCount(); col++) {
        out.writeDouble(mat.get(row, col));
      }
    }
  }

  public static DoubleMatrix read(DataInput in) throws IOException {
    DoubleMatrix mat = new DenseDoubleMatrix(in.readInt(), in.readInt());
    for (int row = 0; row < mat.getRowCount(); row++) {
      for (int col = 0; col < mat.getColumnCount(); col++) {
        mat.set(row, col, in.readDouble());
      }
    }
    return mat;
  }

}
