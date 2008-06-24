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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.FeatureVector;
import org.apache.hama.Matrix;
import org.apache.hama.mapred.MatrixMap;
import org.apache.log4j.Logger;

/**
 * Each map processor requires O(n^2).
 */
public class MultiplicationMap extends MatrixMap<Text, MapWritable> {
  static final Logger LOG = Logger.getLogger(MultiplicationMap.class.getName());

  protected Matrix matrix_b;
  public static final String MATRIX_B = "hama.multiplication.matrix.b";

  public void configure(JobConf job) {
    Text name = new Text(job.get(MATRIX_B, ""));
    matrix_b = new Matrix(new HBaseConfiguration(), name);
  }

  /**
   * @param matrix_a
   * @param matrix_b
   * @param map
   * @param jobConf
   */
  public static void initJob(String matrix_a, String matrix_b,
      Class<MultiplicationMap> map, JobConf jobConf) {
    initJob(matrix_a, map, jobConf);
    jobConf.set(MATRIX_B, matrix_b);
  }

  @Override
  public void map(HStoreKey key, MapWritable value,
      OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException {
    MapWritable multipliedValue = new MapWritable();

    for (Map.Entry<Writable, Writable> e : value.entrySet()) {
      int row = getIndex((Text) e.getKey());
      double c = getDouble(e.getValue());

      FeatureVector v = matrix_b.getRowVector(row);
      for(int i = 0; i < v.size(); i++) {
        // GetColumnText doesn't needed -- edward 
        multipliedValue.put(getColumnText(i), getBytesWritable(v.getValueAt(i) * c));
      }
      
      output.collect(key.getRow(), multipliedValue);
    }
  }
}
