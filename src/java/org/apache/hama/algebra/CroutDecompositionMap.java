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
import org.apache.hama.FractionMatrix;
import org.apache.hama.TriangularMatrix;
import org.apache.hama.mapred.MatrixMap;
import org.apache.hama.mapred.MatrixOutputFormat;
import org.apache.log4j.Logger;

/**
 * Crout Decomposition
 */
public class CroutDecompositionMap extends MatrixMap<Text, MapWritable> {
  static final Logger LOG = Logger.getLogger(CroutDecompositionMap.class
      .getName());
  protected FractionMatrix matrix_a;
  protected TriangularMatrix matrix_lu;

  public static final String MATRIX_A = "crout.decompositionMap.matrix.a";
  public static final String MATRIX_LU = "crout.decompositionMap.matrix.lu";

  public void configure(JobConf job) {
    HBaseConfiguration conf = new HBaseConfiguration();
    Text a_name = new Text(job.get(MATRIX_A, ""));
    Text lu_name = new Text(job.get(MATRIX_LU, ""));
    matrix_a = new FractionMatrix(conf, a_name);
    matrix_lu = new TriangularMatrix(conf, lu_name);
    matrix_lu.setDimension(matrix_a.getRowDimension(), matrix_a
        .getColumnDimension());
  }

  /**
   * @param matrix_a
   * @param matrix_lu
   * @param map
   * @param jobConf
   */
  public static void initJob(String matrix_a, String matrix_lu,
      Class<CroutDecompositionMap> map, JobConf jobConf) {
    initJob(matrix_a, map, jobConf);
    jobConf.set(MatrixOutputFormat.OUTPUT, matrix_lu);
    jobConf.set(MATRIX_A, matrix_a);
    jobConf.set(MATRIX_LU, matrix_lu);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void map(HStoreKey key, MapWritable value,
      OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException {
    Text tKey = key.getRow();
    int i = Integer.parseInt(String.valueOf(tKey.toString()));

    for (Map.Entry<Writable, Writable> e : value.entrySet()) {
      int j = getIndex((Text) e.getKey());
      matrix_lu.setToUpper(i, j, upper(i, j));
      matrix_lu.setToLower(i, j, lower(i, j));
    }

  }

  public double upper(int i, int j) throws IOException {
    double sum;
    if (i == j)
      return 1.0;
    else if (i > j)
      return 0.0;
    else if (i == 0) {
      return matrix_a.getFromOriginal(0, j) / matrix_a.getFromOriginal(0, 0);
    } else {
      sum = 0.0;
      for (int k = 0; k < i; k++) {
        sum += matrix_lu.getFromLower(i, k) * matrix_lu.getFromUpper(k, j);
      }
      double a = matrix_a.getFromOriginal(i, j) - sum;
      double b = matrix_lu.getFromLower(i, i);
      return a / b;
    }
  }

  public double lower(int j, int i) throws IOException {
    double sum, value;
    if (i == 0)
      value = matrix_a.getFromOriginal(j, 0);
    else if (i > j)
      value = 0.0;
    else if (j == i) {
      sum = 0.0;
      for (int k = 0; k < i; k++) {
        sum += matrix_lu.getFromLower(j, k) * matrix_lu.getFromUpper(k, i);
      }
      value = matrix_a.getFromOriginal(i, i) - sum;
    } else {
      sum = 0.0;
      for (int k = 0; k < i; k++) {
        sum += matrix_lu.getFromLower(j, k) * matrix_lu.getFromUpper(k, i);
      }
      value = matrix_a.getFromOriginal(j, i) - sum;
    }
    return value;
  }
}
