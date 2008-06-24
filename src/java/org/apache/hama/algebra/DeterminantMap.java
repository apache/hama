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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.Matrix;
import org.apache.hama.mapred.MatrixMap;
import org.apache.hama.mapred.MatrixOutputFormat;
import org.apache.log4j.Logger;

/**
 * Determinant map
 */
public class DeterminantMap extends MatrixMap<Text, MapWritable> {
  static final Logger LOG = Logger.getLogger(DeterminantMap.class.getName());

  protected Matrix matrix_a;
  protected Matrix checkObj;

  public static final String _CHECK = "hama.determinant.check";
  public static final String MATRIX_A = "hama.determinant.matrix.a";
  protected int row = 0;

  public void configure(JobConf job) {
    HBaseConfiguration conf = new HBaseConfiguration();
    Text name_a = new Text(job.get(MATRIX_A, ""));
    Text check = new Text(job.get(_CHECK, ""));
    
    matrix_a = new Matrix(conf, name_a);
    checkObj = new Matrix(conf, check);
    row = matrix_a.getRowDimension();
  }

  /**
   * @param matrix_a
   * @param check
   * @param map
   * @param jobConf
   */
  public static void initJob(String matrix_a, String check,
      Class<DeterminantMap> map, JobConf jobConf) {
    initJob(matrix_a, map, jobConf);
    jobConf.set(MatrixOutputFormat.OUTPUT, matrix_a);
    jobConf.set(_CHECK, check);
    jobConf.set(MATRIX_A, matrix_a);
  }

  @Override
  public void map(HStoreKey key, MapWritable value,
      OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException {

    int r = Integer.parseInt(String.valueOf(key.getRow()));

    checkObj.set(r, 0,  0.0);
    double d = matrix_a.get(r, 0) * Math.pow(-1.0, r) * minor(r, 1);
    checkObj.set(r, 0, 1.0);

    MapWritable val = new MapWritable();
    val.put(Constants.COLUMN, getBytesWritable(d));
    output.collect(Constants.DETERMINANT, val);
  }

  private double minor(int processRow, int processColumn)
      throws IOException {
    double result = 0.0;
    int i = 0;
    if ((row - processColumn) == 0) {
      return 1.0;
    }
    for (int r = 0; r < row; r++) {
      double trans = checkObj.get(r, 0);
      if (trans != 0.0) {
        checkObj.set(r, 0, 0.0);
        result += matrix_a.get(r, processColumn) * Math.pow(-1.0, i)
            * minor(r, processColumn + 1);
        checkObj.set(r, 0, 1.0);

        i++;
      }
    }
    return result;
  }
}
