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
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.log4j.Logger;

public class RowCyclicAdditionMap extends MapReduceBase implements
Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
  static final Logger LOG = Logger.getLogger(RowCyclicAdditionMap.class);
  protected DenseMatrix[] matrix_summands;
  protected double[] matrix_alphas;
  public static final String MATRIX_SUMMANDS = "hama.addition.summands";
  public static final String MATRIX_ALPHAS = "hama.addition.alphas";

  public void configure(JobConf job) {
    try {
      String[] matrix_names = job.get(MATRIX_SUMMANDS, "").split(","); 
      String[] matrix_alpha_strs = job.get(MATRIX_ALPHAS, "").split(",");
      assert(matrix_names.length == matrix_alpha_strs.length && matrix_names.length >= 1);
      
      matrix_summands = new DenseMatrix[matrix_names.length];
      matrix_alphas = new double[matrix_names.length];
      for(int i=0; i<matrix_names.length; i++) {
        matrix_summands[i] = new DenseMatrix(new HamaConfiguration(job), matrix_names[i]);
        matrix_alphas[i] = Double.valueOf(matrix_alpha_strs[i]);
      }
    } catch (IOException e) {
      LOG.warn("Load matrix_b failed : " + e.getMessage());
    }
  }

  public static void initJob(String matrix_a, String matrix_summandlist, 
      String matrix_alphalist, Class<RowCyclicAdditionMap> map, 
      Class<IntWritable> outputKeyClass, Class<MapWritable> outputValueClass, 
      JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.set(MATRIX_SUMMANDS, matrix_summandlist);
    jobConf.set(MATRIX_ALPHAS, matrix_alphalist);

    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, matrix_a);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, MapWritable value,
      OutputCollector<IntWritable, MapWritable> output, Reporter reporter)
      throws IOException {
    
    DenseVector result = new DenseVector(value);
    DenseVector summand;
    for(int i=0; i<matrix_summands.length; i++) {
      summand = matrix_summands[i].getRow(key.get());
      result = result.add(matrix_alphas[i], summand);
    }
    output.collect(key, result.getEntries());

  }
}
