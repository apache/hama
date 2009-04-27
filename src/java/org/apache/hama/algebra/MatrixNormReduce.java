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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class MatrixNormReduce extends MapReduceBase implements
    Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
  private double max = 0;
  private String outDir = "";
  private JobConf conf;
  private static final String OUTPUT = "hama.multiplication.matrix.a";

  public void configure(JobConf job) {
    outDir = job.get(OUTPUT, "");
    conf = job;
  }

  public static void initJob(String path, Class<MatrixNormReduce> reducer,
      JobConf jobConf) {
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    jobConf.setReducerClass(reducer);
    jobConf.setOutputKeyClass(IntWritable.class);
    jobConf.setOutputValueClass(DoubleWritable.class);
    jobConf.set(OUTPUT, path);
  }

  @Override
  public void reduce(IntWritable key, Iterator<DoubleWritable> values,
      OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
      throws IOException {

    while (values.hasNext()) {
      max = Math.max(values.next().get(), max);
    }

  }

  /**
   * Reduce task done, Writes the largest element of the passed array
   */
  @Override
  public void close() throws IOException {
    // write output to a file
    Path outFile = new Path(outDir, "reduce-out");
    FileSystem fileSys = FileSystem.get(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
        outFile, IntWritable.class, DoubleWritable.class, CompressionType.NONE);
    writer.append(new IntWritable(-1), new DoubleWritable(max));
    writer.close();
  }
}
