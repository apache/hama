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
package org.apache.hama.matrix.algebra;

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
import org.apache.hama.HamaConfiguration;
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.hama.matrix.SparseMatrix;
import org.apache.hama.matrix.SparseVector;
import org.apache.log4j.Logger;

public class SparseMatrixVectorMultMap extends MapReduceBase implements
    Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
  static final Logger LOG = Logger.getLogger(SparseMatrixVectorMultMap.class);
  protected SparseVector currVector;
  public static final String ITH_ROW = "ith.row";
  public static final String MATRIX_A = "hama.multiplication.matrix.a";
  public static final String MATRIX_B = "hama.multiplication.matrix.b";
  private IntWritable nKey = new IntWritable();
  
  public void configure(JobConf job) {
      SparseMatrix matrix_a;
      try {
        matrix_a = new SparseMatrix(new HamaConfiguration(job), job.get(MATRIX_A, ""));
        int ithRow = job.getInt(ITH_ROW, 0);
        nKey.set(ithRow);
        currVector = matrix_a.getRow(ithRow);
      } catch (IOException e) {
        e.printStackTrace();
      }
  }

  public static void initJob(int i, String matrix_a, String matrix_b,
      Class<SparseMatrixVectorMultMap> map, Class<IntWritable> outputKeyClass,
      Class<MapWritable> outputValueClass, JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.setInt(ITH_ROW, i);
    jobConf.set(MATRIX_A, matrix_a);
    jobConf.set(MATRIX_B, matrix_b);
    
    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, matrix_b);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, MapWritable value,
      OutputCollector<IntWritable, MapWritable> output, Reporter reporter)
      throws IOException {

    double ithjth = currVector.get(key.get());
    if(ithjth != 0) {
      SparseVector scaled = new SparseVector(value).scale(ithjth);
      output.collect(nKey, scaled.getEntries());
    }
    
  }
}
