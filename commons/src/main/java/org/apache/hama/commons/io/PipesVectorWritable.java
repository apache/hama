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

import org.apache.hadoop.io.Text;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;

/**
 * VectorWritable for Hama Pipes dense vectors.
 */
public final class PipesVectorWritable extends VectorWritable {

  // private static final Log LOG =
  // LogFactory.getLog(PipesVectorWritable.class);

  public PipesVectorWritable() {
    super();
  }

  public PipesVectorWritable(VectorWritable v) {
    super(v);
  }

  public PipesVectorWritable(DoubleVector v) {
    super(v);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    writeVector(super.getVector(), out);
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    super.set(readVector(in));
  }

  public static void writeVector(DoubleVector vector, DataOutput out)
      throws IOException {
    String str = "";
    for (int i = 0; i < vector.getLength(); i++) {
      str += (i < vector.getLength() - 1) ? vector.get(i) + ", " : vector
          .get(i);
    }

    // LOG.debug("writeVector: '" + str + "'");
    Text.writeString(out, str);
  }

  public static DoubleVector readVector(DataInput in) throws IOException {
    String str = Text.readString(in);
    // LOG.debug("readVector: '" + str + "'");

    String[] values = str.split(",");
    int len = values.length;
    DoubleVector vector = new DenseDoubleVector(len);
    for (int i = 0; i < len; i++) {
      vector.set(i, Double.parseDouble(values[i]));
    }
    return vector;
  }
}
