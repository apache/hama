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
import org.apache.hama.FeatureVector;
import org.apache.hama.Matrix;
import org.apache.hama.TriangularMatrix;
import org.apache.hama.mapred.MatrixMap;
import org.apache.hama.mapred.MatrixOutputFormat;

/**
 * Cholesky Decomposition.
 * <p>
 * For a symmetric, positive definite matrix A, the Cholesky decomposition is an
 * lower triangular matrix L so that A = L*L'.
 * </p>
 */
public class CholeskyDecompositionMap extends MatrixMap<Text, MapWritable> {
  protected Matrix matrix_a;
  protected TriangularMatrix matrix_l;

  public static final String MATRIX_A = "cholesky.decomposition.matrix.a";
  public static final String MATRIX_L = "cholesky.decomposition.matrix.l";

  public void configure(JobConf job) {
    HBaseConfiguration conf = new HBaseConfiguration();
    Text a_name = new Text(job.get(MATRIX_A, ""));
    Text l_name = new Text(job.get(MATRIX_L, ""));
    matrix_a = new Matrix(conf, a_name);
    matrix_l = new TriangularMatrix(conf, l_name);
  }

  /**
   * @param matrix_a
   * @param matrix_l
   * @param map
   * @param jobConf
   */
  public static void initJob(String matrix_a, String matrix_l,
      Class<CholeskyDecompositionMap> map, JobConf jobConf) {
    initJob(matrix_a, map, jobConf);
    jobConf.set(MatrixOutputFormat.OUTPUT, matrix_l);
    jobConf.set(MATRIX_A, matrix_a);
    jobConf.set(MATRIX_L, matrix_l);
  }

  @Override
  public void map(HStoreKey key, MapWritable value,
      OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException {
    int i = Integer.parseInt(String.valueOf(key.getRow()));

    double[] ithRowOfL = new double[matrix_a.getRowDimension()];
    double[] jthRowOfL = new double[matrix_a.getRowDimension()];

    // Vector was added. I don't figure out the result. -- edward
    FeatureVector iOfV = matrix_l.getRowVector(i);
    for(int x = 0; x < iOfV.size(); x++) {
      ithRowOfL[x] = iOfV.getDimAt(x);
    }

    for (int j = i; j < value.size(); j++) {
      FeatureVector jOfV = matrix_l.getRowVector(i);
      for(int x = 0; x < jOfV.size(); x++) {
        jthRowOfL[x] = jOfV.getDimAt(x);
      }

      double sum = matrix_a.get(i, j);
      for (int k = i - 1; k >= 0; k--) {
        sum -= ithRowOfL[k] * jthRowOfL[k];
      }

      if (i == j) {
        matrix_l.setToLower(j, i, Math.sqrt(sum));
      } else {
        double c = matrix_l.get(i, i);
        matrix_l.setToLower(j, i, (sum / c));
      }
    }
  }
}
