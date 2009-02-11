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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseVector;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.VectorWritable;

/**
 * A Map/Reduce help class for blocking a DenseMatrix to a block-formated matrix
 */
public class CollectBlocksMapper extends CollectBlocksBase implements
    CollectBlocksMap<IntWritable, VectorWritable> {

  @Override
  public void map(IntWritable key, VectorWritable value,
      OutputCollector<BlockID, VectorWritable> output, Reporter reporter)
      throws IOException {
    int startColumn;
    int endColumn;
    int blkRow = key.get() / mBlockRowSize;
    DenseVector dv = value.getDenseVector();

    int i = 0;
    do {
      startColumn = i * mBlockColSize;
      endColumn = startColumn + mBlockColSize - 1;
      if (endColumn >= mColumns) // the last sub vector
        endColumn = mColumns - 1;
      output.collect(new BlockID(blkRow, i), new VectorWritable(key.get(), dv
          .subVector(startColumn, endColumn)));

      i++;
    } while (endColumn < (mColumns - 1));
  }

}
