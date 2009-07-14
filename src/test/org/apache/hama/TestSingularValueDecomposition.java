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
package org.apache.hama;

import java.io.IOException;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.algebra.JacobiEigenValue;
import org.apache.hama.util.BytesUtil;
import org.apache.log4j.Logger;

public class TestSingularValueDecomposition extends TestCase {
  static final Logger LOG = Logger.getLogger(TestSingularValueDecomposition.class);
  private static DenseMatrix m1;
  private static HamaConfiguration conf;

  // Let's assume the A = [4 0; 3-5]
  // A'A = [25 -15; -15 25]
  private static double[][] values = { { 25, -15 }, { -15, 25 } };
  // Then, eigenvalues of A'A are 10, 40 
  private static double[] eigenvalues = { 10, 40};
  // And, Singular values are 3.1623, 6.3246
  private static double[] singularvalues = { 3.1622776601683795, 6.324555320336759 };
  
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestSingularValueDecomposition.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        conf = hCluster.getConf();
        m1 = new DenseMatrix(conf, 2, 2);
        for (int i = 0; i < 2; i++)
          for (int j = 0; j < 2; j++)
            m1.set(i, j, values[i][j]);
      }

      protected void tearDown() {
        // do nothing
      }
    };
    return setup;
  }

  public void testLog() throws IOException {
    // Find the eigen/singular values and vectors of A'A    
    m1.jacobiEigenValue(1);
    HTable table = m1.getHTable();
    
    for(int x=0; x<2; x++) {
      double eigenvalue = BytesUtil.bytesToDouble(table.get(BytesUtil.getRowIndex(x), 
          Bytes.toBytes(JacobiEigenValue.EIVAL)).getValue());
      assertTrue(Math.abs(eigenvalues[x] - eigenvalue) < .0000001);
      assertTrue(Math.abs(Math.pow(eigenvalue, 0.5) - singularvalues[x]) < .0000001);
    }
    
    // Therefore, U= AVS'¹=[-0.8944 -0.4472  ; 0.4472 -0.8944]
    // A = USV'=[4 0; 3 -5]
  }
}
