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

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.DenseVector;
import org.apache.hama.Vector;
import org.apache.hama.mapred.DenseMap;
import org.apache.hama.util.Numeric;
import org.apache.log4j.Logger;

public class MultiplicationMap extends DenseMap<IntWritable, DenseVector> {
  static final Logger LOG = Logger.getLogger(MultiplicationMap.class);

  @Override
  public void map(IntWritable key, DenseVector value,
      OutputCollector<IntWritable, DenseVector> output, Reporter reporter)
      throws IOException {

    Iterator<Cell> it = value.iterator();
    int i = 0;
    while (it.hasNext()) {
      Cell c = it.next();
      
      Vector v = MATRIX_B.getRow(i);
      
      double alpha = Numeric.bytesToDouble(c.getValue());
      output.collect(key, (DenseVector) v.scale(alpha));
      i++;
    }
  }
}
