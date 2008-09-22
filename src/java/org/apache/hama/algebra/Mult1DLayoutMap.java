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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.Vector;
import org.apache.hama.io.VectorEntry;
import org.apache.hama.mapred.DenseMap;
import org.apache.log4j.Logger;

/**
 * 1D Block Layout version 
 */
public class Mult1DLayoutMap extends DenseMap<IntWritable, DenseVector> {
  static final Logger LOG = Logger.getLogger(Mult1DLayoutMap.class);
  protected Matrix matrix_b;
  public static final String MATRIX_B = "hama.multiplication.matrix.b";
  
  public void configure(JobConf job) {
    matrix_b = new DenseMatrix(new HamaConfiguration(), job.get(MATRIX_B, ""));
  }
  
  public static void initJob(String matrix_a, String matrix_b,
      Class<Mult1DLayoutMap> map, 
      Class<IntWritable> outputKeyClass, 
      Class<DenseVector> outputValueClass, 
      JobConf jobConf) {
    
    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);
    jobConf.set(MATRIX_B, matrix_b);
    
    initJob(matrix_a, map, jobConf);
  }
  
  @Override
  public void map(IntWritable key, DenseVector value,
      OutputCollector<IntWritable, DenseVector> output, Reporter reporter)
      throws IOException {

    Iterator<VectorEntry> it = value.iterator();
    int i = 0;
    while (it.hasNext()) {
      Vector v = matrix_b.getRow(i);
      output.collect(key, (DenseVector) v.scale(it.next().getValue()));
      i++;
    }
  }
}
