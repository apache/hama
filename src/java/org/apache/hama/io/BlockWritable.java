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
package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.matrix.SubMatrix;

public class BlockWritable implements Writable {
  static final Log LOG = LogFactory.getLog(BlockWritable.class);
  private List<SubMatrix> matrices;

  public BlockWritable() {
    this.matrices = new ArrayList<SubMatrix>();
  }

  public BlockWritable(SubMatrix subMatrix) {
    this.matrices = new ArrayList<SubMatrix>();
    this.matrices.add(subMatrix);
  }

  public void readFields(DataInput in) throws IOException {
    this.matrices.clear();
    int size = in.readInt();

    for (int x = 0; x < size; x++) {
      int rows = in.readInt();
      int columns = in.readInt();

      SubMatrix matrix = new SubMatrix(rows, columns);

      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < columns; j++) {
          matrix.set(i, j, in.readDouble());
        }
      }

      this.matrices.add(matrix);
    }
  }

  public void write(DataOutput out) throws IOException {
    Iterator<SubMatrix> it = this.matrices.iterator();

    int size = this.matrices.size();
    out.writeInt(size);

    while (it.hasNext()) {
      SubMatrix matrix = it.next();

      out.writeInt(matrix.getRows());
      out.writeInt(matrix.getColumns());

      for (int i = 0; i < matrix.getRows(); i++) {
        for (int j = 0; j < matrix.getColumns(); j++) {
          out.writeDouble(matrix.get(i, j));
        }
      }
    }
  }

  public void set(byte[] key, byte[] value) throws IOException {
    int index = 0;
    LOG.info(new String(key));
    
    if (new String(key).equals(Constants.BLOCK + "b")) {
      index = 1;
    }

    this.matrices.add(index, new SubMatrix(value));
  }

  public Iterator<SubMatrix> getMatrices() {
    return this.matrices.iterator();
  }
  
  public SubMatrix get(int index) {
    return this.matrices.get(index);
  }
  
  public int size() {
    return this.matrices.size();
  }
}
