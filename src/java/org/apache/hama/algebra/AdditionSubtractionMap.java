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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.Matrix;
import org.apache.hama.mapred.MatrixMap;
import org.apache.log4j.Logger;

/**
 * Matrix addition/subtraction map
 */
public class AdditionSubtractionMap extends MatrixMap<Text, MapWritable> {
  static final Logger LOG = Logger.getLogger(AdditionSubtractionMap.class);
  
  protected Matrix matrix_b;
  protected String operator;
  public static final String MATRIX_B = "hama.addition.substraction.matrix.b";
  public static final String OPERATOR = "hama.addition.substraction.operator";

  public void configure(JobConf job) {
    operator = job.get(OPERATOR, "");
    Text b_name = new Text(job.get(MATRIX_B, ""));
    matrix_b = new Matrix(new HBaseConfiguration(), b_name);
  }

  public static void initJob(String matrix_a, String matrix_b, String operator,
      Class<AdditionSubtractionMap> map, JobConf jobConf) {
    initJob(matrix_a, map, jobConf);
    jobConf.set(MATRIX_B, matrix_b);
    jobConf.set(OPERATOR, operator);
  }

  @Override
  public void map(HStoreKey key, MapWritable value,
      OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException {
    Text tKey = key.getRow();
    MapWritable sum = new MapWritable();

    for (Map.Entry<Writable, Writable> e : value.entrySet()) {
      double a = getDouble(e.getValue());
      double b = matrix_b.get(Integer.parseInt(tKey.toString()),
          getIndex((Text) e.getKey()));
      byte[] result = null;

      if (operator.equals(Constants.PLUS)) {
        result = toBytes(a + b);
      } else if (operator.equals(Constants.MINUS)) {
        result = toBytes(a - b);
      }

      sum.put(e.getKey(), new ImmutableBytesWritable(result));
    }
    output.collect(tKey, sum);
  }
}
