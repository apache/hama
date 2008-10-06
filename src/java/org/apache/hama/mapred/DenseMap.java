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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.Matrix;
import org.apache.hama.io.VectorWritable;

@SuppressWarnings("unchecked")
public abstract class DenseMap<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements
    Mapper<IntWritable, VectorWritable, K, V> {
  public static Matrix MATRIX_B;

  public static void initJob(String matrixA, 
      Class<? extends DenseMap> mapper,
      JobConf job) {

    job.setInputFormat(MatrixInputFormat.class);
    job.setMapperClass(mapper);
    FileInputFormat.addInputPaths(job, matrixA);

    job.set(MatrixInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  public abstract void map(IntWritable key, VectorWritable value,
      OutputCollector<K, V> output, Reporter reporter) throws IOException;
}
