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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.mapred.RowCyclicReduce;
import org.apache.log4j.Logger;

public class Mult1DLayoutReduce extends
    RowCyclicReduce<IntWritable, VectorWritable> {
  static final Logger LOG = Logger.getLogger(Mult1DLayoutReduce.class);
  public static final Map<Integer, Double> buffer = new HashMap<Integer, Double>();
  
  @Override
  public void reduce(IntWritable key, Iterator<VectorWritable> values,
      OutputCollector<IntWritable, VectorUpdate> output, Reporter reporter)
      throws IOException {

    VectorUpdate update = new VectorUpdate(key.get());
    VectorWritable sum;
    
    // Summation
    while (values.hasNext()) {
      sum = values.next();
      for (int i = 0; i < sum.size(); i++) {
        if (buffer.containsKey(i)) {
          buffer.put(i, sum.get(i).getValue() + buffer.get(i));
        } else {
          buffer.put(i, sum.get(i).getValue());
        }
      }
    }
    
    update.putAll(buffer);
    buffer.clear();
    output.collect(key, update);
  }

}
