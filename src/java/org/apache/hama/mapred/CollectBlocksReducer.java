/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hama.mapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseVector;
import org.apache.hama.SubMatrix;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;

/**
 * Rows are named as c(i, j) with sequential number ((N^2 * i) + ((j * N) + k)
 * to avoid duplicated records. Each row has a two sub matrices of a(i, k) and
 * b(k, j).
 */
public class CollectBlocksReducer extends CollectBlocksMapReduceBase implements
    Reducer<BlockID, MapWritable, BlockID, BlockWritable> {
  static final Log LOG = LogFactory.getLog(CollectBlocksReducer.class);
  
  @Override
  public void reduce(BlockID key, Iterator<MapWritable> values,
      OutputCollector<BlockID, BlockWritable> output, Reporter reporter)
      throws IOException {
    // Note: all the sub-vectors are grouped by {@link
    // org.apache.hama.io.BlockID}

    // the block's base offset in the original matrix
    int colBase = key.getColumn() * mBlockColSize;
    int rowBase = key.getRow() * mBlockRowSize;

    // the block's size : rows & columns
    int smRows = mBlockRowSize;
    if ((rowBase + mBlockRowSize - 1) >= mRows)
      smRows = mRows - rowBase;
    int smCols = mBlockColSize;
    if ((colBase + mBlockColSize - 1) >= mColumns)
      smCols = mColumns - colBase;

    // construct the matrix
    SubMatrix subMatrix = new SubMatrix(smRows, smCols);

    // i, j is the current offset in the sub-matrix
    int i = 0, j = 0;
    while (values.hasNext()) {
      DenseVector vw = new DenseVector(values.next());
      // check the size is suitable
      if (vw.size() != smCols)
        throw new IOException("Block Column Size dismatched.");
      i = vw.getRow() - rowBase;
      
      if (i >= smRows || i < 0)
        throw new IOException("Block Row Size dismatched.");

      // put the subVector to the subMatrix
      for (j = 0; j < smCols; j++) {
        subMatrix.set(i, j, vw.get(colBase + j));
      }
    }
    BlockWritable outValue = new BlockWritable(subMatrix);

    // It'll used for only matrix multiplication.
    if (matrixPos) {
      for (int x = 0; x < mBlockNum; x++) {
        int r = (key.getRow() * mBlockNum) * mBlockNum;
        int seq = (x * mBlockNum) + key.getColumn() + r;
        output.collect(new BlockID(key.getRow(), x, seq), outValue);
      }
    } else {
      for (int x = 0; x < mBlockNum; x++) {
        int seq = (x * mBlockNum * mBlockNum) + (key.getColumn() * mBlockNum)
            + key.getRow();
        output.collect(new BlockID(x, key.getColumn(), seq), outValue);
      }
    }
  }
}
