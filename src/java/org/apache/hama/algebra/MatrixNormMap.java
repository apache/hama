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
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.mapred.VectorInputFormat;

public class MatrixNormMap extends MapReduceBase implements
    Mapper<IntWritable, MapWritable, IntWritable, DoubleWritable> {
  private IntWritable nKey = new IntWritable(-1);
  private DoubleWritable nValue = new DoubleWritable();
  
  public static void initJob(String path, Class<MatrixNormMap> map,
      Class<IntWritable> outputKeyClass, Class<DoubleWritable> outputValueClass,
      JobConf jobConf) {
    jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeyClass(outputKeyClass);
    jobConf.setMapperClass(map);

    jobConf.setInputFormat(VectorInputFormat.class);
    FileInputFormat.addInputPaths(jobConf, path);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }

  @Override
  public void map(IntWritable key, MapWritable value,
      OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
      throws IOException {

    double rowSum = 0;
    for(Map.Entry<Writable, Writable> e : value.entrySet()) {
      rowSum += Math.abs(((DoubleEntry) e.getValue()).getValue());
    }
    nValue.set(rowSum);
    
    output.collect(nKey, nValue);
  }

}
