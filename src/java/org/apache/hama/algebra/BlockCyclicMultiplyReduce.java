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
import org.apache.hama.SubMatrix;
import org.apache.hama.io.BlockEntry;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.BlockCyclicReduce;
import org.apache.log4j.Logger;

public class BlockCyclicMultiplyReduce extends
    BlockCyclicReduce<IntWritable, BlockWritable> {
  static final Logger LOG = Logger.getLogger(BlockCyclicMultiplyReduce.class);

  @Override
  public void reduce(IntWritable key, Iterator<BlockWritable> values,
      OutputCollector<IntWritable, VectorUpdate> output, Reporter reporter)
      throws IOException {
    int row = key.get();
    Map<Integer, SubMatrix> sum = new HashMap<Integer, SubMatrix>();

    while (values.hasNext()) {
      BlockWritable b = values.next();
      for (Map.Entry<Integer, BlockEntry> e : b.entrySet()) {
        int j = e.getKey();
        SubMatrix value = e.getValue().getValue();
        if (sum.containsKey(j)) {
          sum.put(j, sum.get(j).add(value));
        } else {
          sum.put(j, value);
        }
      }
    }

    for (Map.Entry<Integer, SubMatrix> e : sum.entrySet()) {
      int column = e.getKey();
      SubMatrix mat = e.getValue();

      int startRow = row * mat.size();
      int startColumn = column * mat.size();

      // TODO: sub matrix can be not a regular sqaure
      for (int i = 0; i < mat.size(); i++) {
        VectorUpdate update = new VectorUpdate(i + startRow);
        for (int j = 0; j < mat.size(); j++) {
          update.put(j + startColumn, mat.get(i, j));
        }
        output.collect(key, update);
      }
    }
  }
}
