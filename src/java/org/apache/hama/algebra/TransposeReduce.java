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
import java.util.Map;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.VectorOutputFormat;

public class TransposeReduce  extends MapReduceBase implements
Reducer<IntWritable, MapWritable, IntWritable, VectorUpdate> {

  public static void initJob(String path, Class<TransposeReduce> reducer,
      JobConf jobConf) {
    jobConf.setOutputFormat(VectorOutputFormat.class);
    jobConf.setReducerClass(reducer);
    jobConf.set(VectorOutputFormat.OUTPUT_TABLE, path);
    jobConf.setOutputKeyClass(IntWritable.class);
    jobConf.setOutputValueClass(BatchUpdate.class);
  }

  @Override
  public void reduce(IntWritable key, Iterator<MapWritable> values,
      OutputCollector<IntWritable, VectorUpdate> output, Reporter reporter)
      throws IOException {

    MapWritable sum = new MapWritable();
    while (values.hasNext()) {
      for(Map.Entry<Writable, Writable> e: values.next().entrySet()) {
        sum.put(e.getKey(), e.getValue());  
      }
    }
    
    VectorUpdate update = new VectorUpdate(key.get());
    update.putAll(sum);
    output.collect(key, update);
  }
}
