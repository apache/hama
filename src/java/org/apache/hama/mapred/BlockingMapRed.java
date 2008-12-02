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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hama.Constants;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.SubMatrix;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.VectorWritable;

/**
 * A Map/Reduce help class for blocking a DenseMatrix to a block-formated matrix
 */
public class BlockingMapRed {

  static final Log LOG = LogFactory.getLog(BlockingMapRed.class);
  /** Parameter of the path of the matrix to be blocked * */
  public static final String BLOCKING_MATRIX = "hama.blocking.matrix";

  /**
   * Initialize a job to blocking a table
   * 
   * @param matrixPath
   * @param job
   */
  public static void initJob(String matrixPath, JobConf job) {
    job.setMapperClass(BlockingMapper.class);
    job.setReducerClass(BlockingReducer.class);
    FileInputFormat.addInputPaths(job, matrixPath);

    job.setInputFormat(VectorInputFormat.class);
    job.setMapOutputKeyClass(BlockID.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setOutputFormat(NullOutputFormat.class);

    job.set(BLOCKING_MATRIX, matrixPath);
    job.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  /**
   * Abstract Blocking Map/Reduce Class to configure the job.
   */
  public static abstract class BlockingMapRedBase extends MapReduceBase {

    protected DenseMatrix matrix;
    protected int mBlockNum;
    protected int mBlockRowSize;
    protected int mBlockColSize;

    @Override
    public void configure(JobConf job) {
      try {
        matrix = new DenseMatrix(new HamaConfiguration(), job.get(
            BLOCKING_MATRIX, ""));
        mBlockNum = matrix.getBlockSize();
        mBlockRowSize = matrix.getRows() / mBlockNum;
        mBlockColSize = matrix.getColumns() / mBlockNum;
      } catch (IOException e) {
        LOG.warn("Load matrix_blocking failed : " + e.getMessage());
      }
    }

  }

  /**
   * Mapper Class
   */
  public static class BlockingMapper extends BlockingMapRedBase implements
      Mapper<IntWritable, VectorWritable, BlockID, VectorWritable> {

    @Override
    public void map(IntWritable key, VectorWritable value,
        OutputCollector<BlockID, VectorWritable> output, Reporter reporter)
        throws IOException {
      int startColumn;
      int endColumn;
      int blkRow = key.get() / mBlockRowSize;
      DenseVector dv = value.getDenseVector();
      for (int i = 0; i < mBlockNum; i++) {
        startColumn = i * mBlockColSize;
        endColumn = startColumn + mBlockColSize - 1;
        output.collect(new BlockID(blkRow, i), new VectorWritable(key.get(),
            (DenseVector) dv.subVector(startColumn, endColumn)));
      }
    }

  }

  /**
   * Reducer Class
   */
  public static class BlockingReducer extends BlockingMapRedBase implements
      Reducer<BlockID, VectorWritable, BlockID, SubMatrix> {

    @Override
    public void reduce(BlockID key, Iterator<VectorWritable> values,
        OutputCollector<BlockID, SubMatrix> output, Reporter reporter)
        throws IOException {
      // Note: all the sub-vectors are grouped by {@link
      // org.apache.hama.io.BlockID}
      SubMatrix subMatrix = new SubMatrix(mBlockRowSize, mBlockColSize);
      int i = 0, j = 0;
      int colBase = key.getColumn() * mBlockColSize;
      int rowBase = key.getRow() * mBlockRowSize;
      while (values.hasNext()) {
        VectorWritable vw = values.next();
        // check the size is suitable
        if (vw.size() != mBlockColSize)
          throw new IOException("BlockColumnSize dismatched.");
        i = vw.row - rowBase;
        if (i >= mBlockRowSize || i < 0)
          throw new IOException("BlockRowSize dismatched.");

        // put the subVector to the subMatrix
        for (j = 0; j < mBlockColSize; j++) {
          subMatrix.set(i, j, vw.get(colBase + j));
        }
      }

      matrix.setBlock(key.getRow(), key.getColumn(), subMatrix);
    }
  }

}
