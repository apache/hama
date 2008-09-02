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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseVector;
import org.apache.hama.mapred.MatrixReduce;
import org.apache.hama.util.Numeric;
import org.apache.log4j.Logger;

public class MultiplicationReduce extends
    MatrixReduce<IntWritable, DenseVector> {
  static final Logger LOG = Logger.getLogger(MultiplicationReduce.class);

  @Override
  public void reduce(IntWritable key, Iterator<DenseVector> values,
      OutputCollector<IntWritable, BatchUpdate> output, Reporter reporter)
      throws IOException {

    BatchUpdate b = new BatchUpdate(Numeric.intToBytes(key.get()));
    DenseVector sum;
    Map<Integer, Double> buffer = new HashMap<Integer, Double>();

    // Summation vectors
    while (values.hasNext()) {
      sum = values.next();
      for (int i = 0; i < sum.size(); i++) {
        if (buffer.containsKey(i)) {
          buffer.put(i, sum.get(i) + buffer.get(i));
        } else {
          buffer.put(i, sum.get(i));
        }
      }
    }

    for (Map.Entry<Integer, Double> f : buffer.entrySet()) {
      byte[] value = Numeric.doubleToBytes(f.getValue());
      b.put(Numeric.getColumnIndex(f.getKey()), value);
    }

    output.collect(key, b);
  }

}
