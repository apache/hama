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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseVector;
import org.apache.hama.SparseVector;
import org.apache.hama.Vector;
import org.apache.hama.util.RandomVariable;
import org.apache.log4j.Logger;

/**
 * Generate matrix with random elements
 */
public class RandomMatrixMap extends MapReduceBase implements
    Mapper<IntWritable, IntWritable, IntWritable, MapWritable> {
  static final Logger LOG = Logger.getLogger(RandomMatrixMap.class);
  protected int column, density;
  protected String type;
  protected Vector vector = new DenseVector();

  @Override
  public void map(IntWritable key, IntWritable value,
      OutputCollector<IntWritable, MapWritable> output, Reporter report)
      throws IOException {
    if (type.equals("SparseMatrix")) {
      ((SparseVector) vector).clear();
      for (int i = key.get(); i <= value.get(); i++) {
        for (int j = 0; j < column; j++) {
            ((SparseVector) vector).set(j, RandomVariable.rand(density));
        }
        output.collect(new IntWritable(i), vector.getEntries());
      }
    } else {
      ((DenseVector) vector).clear();
      for (int i = key.get(); i <= value.get(); i++) {
        for (int j = 0; j < column; j++) {
          ((DenseVector) vector).set(j, RandomVariable.rand());
        }
        output.collect(new IntWritable(i), vector.getEntries());
      }
    }
  }

  public void configure(JobConf job) {
    column = job.getInt("matrix.column", 0);
    density = job.getInt("matrix.density", 100);
    type = job.get("matrix.type");
    if (type.equals("SparseMatrix"))
      vector = new SparseVector();
    else
      vector = new DenseVector();
  }
}
