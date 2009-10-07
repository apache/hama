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
package org.apache.hama.matrix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.algebra.JacobiEigenValue;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

public class TestSingularValueDecomposition extends HamaCluster {
  static final Logger LOG = Logger
      .getLogger(TestSingularValueDecomposition.class);
  private DenseMatrix m1;
  private HamaConfiguration conf;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestSingularValueDecomposition() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();
    m1 = new DenseMatrix(conf, 2, 2);
    for (int i = 0; i < 2; i++)
      for (int j = 0; j < 2; j++)
        m1.set(i, j, matrixA[i][j]);
  }

  // Let's assume the A = [4 0; 3-5]
  private double[][] matrixA = { { 4, 0 }, { 3, -5 } };
  // A'A = [25 -15; -15 25]
  private double[][] values = { { 25, -15 }, { -15, 25 } };
  // Then, eigenvalues of A'A are 10, 40
  private double[] eigenvalues = { 10, 40 };
  // And, Singular values are 3.1623, 6.3246
  private double[] singularvalues = { 3.1622776601683795, 6.324555320336759 };

  public void testEigenSingularValues() throws IOException {
    Matrix aT = m1.transpose();
    DenseMatrix aTa = (DenseMatrix) aT.mult(m1);

    for (int i = 0; i < m1.getRows(); i++) {
      for (int j = 0; j < m1.getRows(); j++) {
        assertEquals(aTa.get(i, j), values[i][j]);
      }
    }

    // Find the eigen/singular values and vectors of A'A
    aTa.jacobiEigenValue(1);
    HTable table = aTa.getHTable();

    for (int x = 0; x < 2; x++) {
      Get get = new Get(BytesUtil.getRowIndex(x));
      get.addColumn(Bytes.toBytes(JacobiEigenValue.EIVAL));
      double eigenvalue = BytesUtil.bytesToDouble(table.get(get).getCellValue()
          .getValue());
      assertTrue(Math.abs(eigenvalues[x] - eigenvalue) < .0000001);
      assertTrue(Math.abs(Math.pow(eigenvalue, 0.5) - singularvalues[x]) < .0000001);
    }

    // TODO: need to compute the inverse of S, S(-1)
    // TODO: need to find out the V(T)

    // Therefore, U= AVS'1=[-0.8944 -0.4472; 0.4472 -0.8944]
    // A = USV'=[4 0; 3 -5]
  }
}
