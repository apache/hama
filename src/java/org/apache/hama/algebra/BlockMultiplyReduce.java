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
package org.apache.hama.algebra;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.SubMatrix;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.VectorOutputFormat;
import org.apache.log4j.Logger;

public class BlockMultiplyReduce extends MapReduceBase implements
    Reducer<BlockID, BlockWritable, IntWritable, VectorUpdate> {
  static final Logger LOG = Logger.getLogger(BlockMultiplyReduce.class);

  /**
   * Use this before submitting a BlockCyclicMultiplyReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initJob(String table,
      Class<BlockMultiplyReduce> reducer, JobConf job) {
    job.setOutputFormat(VectorOutputFormat.class);
    job.setReducerClass(reducer);
    job.set(VectorOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BatchUpdate.class);
  }

  @Override
  public void reduce(BlockID key, Iterator<BlockWritable> values,
      OutputCollector<IntWritable, VectorUpdate> output, Reporter reporter)
      throws IOException {

    SubMatrix s = null;
    while (values.hasNext()) {
      SubMatrix b = values.next().getMatrices().next();
      if (s == null) {
        s = b;
      } else {
        s = s.add(b);
      }
    }

    int startRow = key.getRow() * s.getRows();
    int startColumn = key.getColumn() * s.getColumns();

    for (int i = 0; i < s.getRows(); i++) {
      VectorUpdate update = new VectorUpdate(i + startRow);
      for (int j = 0; j < s.getColumns(); j++) {
        update.put(j + startColumn, s.get(i, j));
      }
      output.collect(new IntWritable(key.getRow()), update);
    }
  }
}
