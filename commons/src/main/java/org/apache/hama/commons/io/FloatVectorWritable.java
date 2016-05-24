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
package org.apache.hama.commons.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.commons.math.DenseFloatVector;
import org.apache.hama.commons.math.FloatVector;
import org.apache.hama.commons.math.NamedFloatVector;

public class FloatVectorWritable implements WritableComparable<FloatVectorWritable> {

  private FloatVector vector;

  public FloatVectorWritable() {
    super();
  }

  public FloatVectorWritable(FloatVectorWritable v) {
    this.vector = v.getVector();
  }

  public FloatVectorWritable(FloatVector v) {
    this.vector = v;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeVector(this.vector, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.vector = readVector(in);
  }

  @Override
  public final int compareTo(FloatVectorWritable o) {
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
    FloatVectorWritable other = (FloatVectorWritable) obj;
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
  public FloatVector getVector() {
    return vector;
  }

  @Override
  public String toString() {
    return vector.toString();
  }

  public static void writeVector(FloatVector vector, DataOutput out)
      throws IOException {
    out.writeInt(vector.getLength());
    for (int i = 0; i < vector.getDimension(); i++) {
      out.writeFloat(vector.get(i));
    }

    if (vector.isNamed() && vector.getName() != null) {
      out.writeBoolean(true);
      out.writeUTF(vector.getName());
    } else {
      out.writeBoolean(false);
    }
  }

  public static FloatVector readVector(DataInput in) throws IOException {
    int length = in.readInt();
    FloatVector vector;
    vector = new DenseFloatVector(length);
    for (int i = 0; i < length; i++) {
      vector.set(i, in.readFloat());
    }

    if (in.readBoolean()) {
      vector = new NamedFloatVector(in.readUTF(), vector);
    }
    return vector;
  }

  public static int compareVector(FloatVectorWritable a, FloatVectorWritable o) {
    return compareVector(a.getVector(), o.getVector());
  }

  public static int compareVector(FloatVector a, FloatVector o) {
    FloatVector subtract = a.subtractUnsafe(o);
    return (int) subtract.sum();
  }

  public static FloatVectorWritable wrap(FloatVector a) {
    return new FloatVectorWritable(a);
  }

  public void set(FloatVector vector) {
    this.vector = vector;
  }
}
