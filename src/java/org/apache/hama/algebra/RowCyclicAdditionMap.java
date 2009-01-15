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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.DenseMatrix;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.log4j.Logger;

public class RowCyclicAdditionMap extends MapReduceBase implements
Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
  static final Logger LOG = Logger.getLogger(RowCyclicAdditionMap.class);
  protected DenseMatrix matrix_b;
  public static final String MATRIX_B = "hama.addition.matrix.b";

  public void configure(JobConf job) {
    try {
      matrix_b = new DenseMatrix(new HamaConfiguration(job), job.get(MATRIX_B, ""));
    } catch (IOException e) {
      LOG.warn("Load matrix_b failed : " + e.getMessage());
    }
  }

  public static void initJob(String matrix_a, String matrix_b,
      Class<RowCyclicAdditionMap> map, Class<IntWritable> outputKeyClass,
      Class<VectorWritable> outputValueClass, JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.set(MATRIX_B, matrix_b);


    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, matrix_a);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, VectorWritable value,
      OutputCollector<IntWritable, VectorWritable> output, Reporter reporter)
      throws IOException {

    output.collect(key, new VectorWritable(key.get(), 
        matrix_b.getRow(key.get()).add(value.getDenseVector())));

  }
}
