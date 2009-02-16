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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;

/**
 * Abstract Blocking Map/Reduce Class to configure the job.
 */
public abstract class CollectBlocksMapReduceBase extends MapReduceBase {
  /** Parameter of the path of the matrix to be blocked * */
  public static final String BLOCK_SIZE = "hama.blocking.size";
  public static final String ROWS = "hama.blocking.rows";
  public static final String COLUMNS = "hama.blocking.columns";
  public static final String MATRIX_POS = "a.ore.b";

  protected int mBlockNum;
  protected int mBlockRowSize;
  protected int mBlockColSize;
  protected int mRows;
  protected int mColumns;
  protected boolean matrixPos;

  @Override
  public void configure(JobConf job) {
    mBlockNum = Integer.parseInt(job.get(BLOCK_SIZE, ""));
    mRows = Integer.parseInt(job.get(ROWS, ""));
    mColumns = Integer.parseInt(job.get(COLUMNS, ""));

    mBlockRowSize = mRows / mBlockNum;
    mBlockColSize = mColumns / mBlockNum;

    matrixPos = job.getBoolean(MATRIX_POS, true);
  }

  /**
   * Initialize a job to blocking a table
   */
  @SuppressWarnings("unchecked")
  public static void initJob(String collectionTable, boolean bool,
      int block_size, int i, int j, JobConf job) {
    job.setReducerClass(CollectBlocksReducer.class);
    job.setMapOutputKeyClass(BlockID.class);
    job.setMapOutputValueClass(MapWritable.class);

    job.setOutputFormat(BlockOutputFormat.class);
    job.setOutputKeyClass(BlockID.class);
    job.setOutputValueClass(BlockWritable.class);
    job.set(BLOCK_SIZE, String.valueOf(block_size));
    job.set(ROWS, String.valueOf(i));
    job.set(COLUMNS, String.valueOf(j));
    job.setBoolean(MATRIX_POS, bool);
    job.set(BlockOutputFormat.OUTPUT_TABLE, collectionTable);

    if (bool)
      job.set(BlockOutputFormat.COLUMN, "a");
    else
      job.set(BlockOutputFormat.COLUMN, "b");
  }
}
