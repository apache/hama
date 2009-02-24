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
import org.apache.hama.SparseMatrix;
import org.apache.hama.SparseVector;
import org.apache.hama.Vector;
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.log4j.Logger;

/**
 * SIMD version
 */
public class SIMDMultiplyMap extends MapReduceBase implements
Mapper<IntWritable, MapWritable, IntWritable, MapWritable> {
  static final Logger LOG = Logger.getLogger(SIMDMultiplyMap.class);
  protected Matrix matrix_b;
  protected Vector sum;
  protected String type;
  public static final String MATRIX_B = "hama.multiplication.matrix.b";
  public static final String MATRIX_TYPE = "hama.multiplication.matrix.type";
  
  public void configure(JobConf job) {
    try {
      type = job.get(MATRIX_TYPE, "");
      if(type.equals("SparseMatrix")) {
        matrix_b = new SparseMatrix(new HamaConfiguration(job), job.get(MATRIX_B, ""));
        sum = new SparseVector();
      } else {
        matrix_b = new DenseMatrix(new HamaConfiguration(job), job.get(MATRIX_B, ""));
        sum = new DenseVector();
      }
    } catch (IOException e) {
      LOG.warn("Load matrix_b failed : " + e.getMessage());
    }
  }

  public static void initJob(String matrix_a, String matrix_b, String type, 
      Class<SIMDMultiplyMap> map, Class<IntWritable> outputKeyClass,
      Class<MapWritable> outputValueClass, JobConf jobConf) {

    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.set(MATRIX_B, matrix_b);
    jobConf.set(MATRIX_TYPE, type);

    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, matrix_a);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, MapWritable value,
      OutputCollector<IntWritable, MapWritable> output, Reporter reporter)
      throws IOException {
    if(type.equals("SparseMatrix")) {
      ((SparseVector) sum).clear();
      SparseVector currVector = new SparseVector(value);
      
      for(int i = 0; i < matrix_b.getColumns(); i++) {
        ((SparseVector) sum).add(((SparseMatrix) matrix_b).getRow(i).scale(
            currVector.get(i)));
      }
      
      
      output.collect(key, ((SparseVector) sum).getEntries());
    } else {
      ((DenseVector) sum).clear();
      DenseVector currVector = new DenseVector(value);
      for(int i = 0; i < value.size(); i++) {
        ((DenseVector) sum).add(((DenseMatrix) matrix_b).getRow(i).scale(
            currVector.get(i)));
      }
      output.collect(key, ((DenseVector) sum).getEntries());
    }
    
  }
}
