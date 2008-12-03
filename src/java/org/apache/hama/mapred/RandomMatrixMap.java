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
package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.util.RandomVariable;
import org.apache.log4j.Logger;

/**
 * Generate matrix with random elements
 */
public class RandomMatrixMap extends MapReduceBase implements
    Mapper<IntWritable, IntWritable, BooleanWritable, LongWritable> {
  static final Logger LOG = Logger.getLogger(RandomMatrixMap.class);
  protected Matrix matrix;
  protected int column;

  @Override
  public void map(IntWritable key, IntWritable value,
      OutputCollector<BooleanWritable, LongWritable> output, Reporter report)
      throws IOException {
    DenseVector vector = new DenseVector();
    for (int i = key.get(); i <= value.get(); i++) {
      vector.clear();
      for (int j = 0; j < column; j++) {
        vector.set(j, RandomVariable.rand());
      }
      matrix.setRow(i, vector);
    }
  }

  public void configure(JobConf job) {
    try {
      column = Integer.parseInt(job.get("matrix.column"));
      matrix = new DenseMatrix(new HamaConfiguration(), job.get("matrix.path"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
