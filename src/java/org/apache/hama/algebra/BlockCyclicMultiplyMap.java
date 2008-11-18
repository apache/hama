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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseMatrix;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.SubMatrix;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.mapred.BlockCyclicMap;
import org.apache.log4j.Logger;

public class BlockCyclicMultiplyMap extends
    BlockCyclicMap<IntWritable, BlockWritable> {
  static final Logger LOG = Logger.getLogger(BlockCyclicMultiplyMap.class);
  protected Matrix matrix_b;
  public static final String MATRIX_B = "hama.multiplication.matrix.b";

  public void configure(JobConf job) {
    try {
      matrix_b = new DenseMatrix(new HamaConfiguration(), job.get(MATRIX_B, ""));
    } catch (IOException e) {
      LOG.warn("Load matrix_b failed : " + e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  public static void initJob(String matrix_a, String matrix_b,
      Class<BlockCyclicMultiplyMap> map, Class<IntWritable> outputKeyClass,
      Class<BlockWritable> outputValueClass, JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.set(MATRIX_B, matrix_b);

    initJob(matrix_a, map, jobConf);
  }

  @Override
  public void map(IntWritable key, BlockWritable value,
      OutputCollector<IntWritable, BlockWritable> output, Reporter reporter)
      throws IOException {
    for (int i = 0; i < value.size(); i++) {
      SubMatrix a = value.get(i);
      for (int j = 0; j < ((DenseMatrix) matrix_b).getBlockSize(); j++) {
        SubMatrix b = ((DenseMatrix) matrix_b).getBlock(i, j);
        SubMatrix c = a.mult(b);
        output.collect(key, new BlockWritable(key.get(), j, c));
      }
    }
  }
}
