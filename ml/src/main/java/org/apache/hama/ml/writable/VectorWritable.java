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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;

/**
 * Writable for dense vectors.
 */
public final class VectorWritable implements WritableComparable<VectorWritable> {

  private DoubleVector vector;

  public VectorWritable() {
    super();
  }

  public VectorWritable(VectorWritable v) {
    this.vector = v.getVector();
  }

  public VectorWritable(DoubleVector v) {
    this.vector = v;
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    writeVector(this.vector, out);
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    this.vector = readVector(in);
  }

  @Override
  public final int compareTo(VectorWritable o) {
    return compareVector(this, o);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((vector == null) ? 0 : vector.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VectorWritable other = (VectorWritable) obj;
    if (vector == null) {
      if (other.vector != null)
        return false;
    } else if (!vector.equals(other.vector))
      return false;
    return true;
  }

  /**
   * @return the embedded vector
   */
  public DoubleVector getVector() {
    return vector;
  }

  @Override
  public String toString() {
    return vector.toString();
  }

  public static void writeVector(DoubleVector vector, DataOutput out)
      throws IOException {
    out.writeInt(vector.getLength());
    for (int i = 0; i < vector.getDimension(); i++) {
      out.writeDouble(vector.get(i));
    }
  }

  public static DoubleVector readVector(DataInput in) throws IOException {
    int length = in.readInt();
    DoubleVector vector;
    vector = new DenseDoubleVector(length);
    for (int i = 0; i < length; i++) {
      vector.set(i, in.readDouble());
    }
    return vector;
  }

  public static int compareVector(VectorWritable a, VectorWritable o) {
    return compareVector(a.getVector(), o.getVector());
  }

  public static int compareVector(DoubleVector a, DoubleVector o) {
    DoubleVector subtract = a.subtract(o);
    return (int) subtract.sum();
  }

  public static VectorWritable wrap(DoubleVector a) {
    return new VectorWritable(a);
  }

  public void set(DoubleVector vector) {
    this.vector = vector;
  }
}
