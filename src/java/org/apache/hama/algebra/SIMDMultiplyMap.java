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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.log4j.Logger;

/**
 * SIMD version
 */
public class SIMDMultiplyMap extends MapReduceBase implements
Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
  static final Logger LOG = Logger.getLogger(SIMDMultiplyMap.class);
  protected Matrix matrix_b;
  public static final String MATRIX_B = "hama.multiplication.matrix.b";
  public static final DenseVector sum = new DenseVector();;
  
  public void configure(JobConf job) {
    try {
      matrix_b = new DenseMatrix(new HamaConfiguration(job), job.get(MATRIX_B, ""));
    } catch (IOException e) {
      LOG.warn("Load matrix_b failed : " + e.getMessage());
    }
  }

  public static void initJob(String matrix_a, String matrix_b,
      Class<SIMDMultiplyMap> map, Class<IntWritable> outputKeyClass,
      Class<MapWritable> outputValueClass, JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.set(MATRIX_B, matrix_b);

    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, matrix_a);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, MapWritable value,
      OutputCollector<IntWritable, MapWritable> output, Reporter reporter)
      throws IOException {
    sum.clear();

    for(int i = 0; i < value.size(); i++) {
      sum.add(matrix_b.getRow(i).scale(((DoubleEntry) value.get(new IntWritable(i))).getValue()));
    }
    
    output.collect(key, sum.getEntries());
  }
}
