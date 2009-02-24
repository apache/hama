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

import org.apache.log4j.Logger;

public class TestSparseMatrix extends TestCase {
  static final Logger LOG = Logger.getLogger(TestSparseMatrix.class);
  private static int SIZE = 10;
  private static SparseMatrix m1;
  private static SparseMatrix m2;

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestSparseMatrix.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        m1 = SparseMatrix.random(hCluster.getConf(), SIZE, SIZE);
        m2 = SparseMatrix.random(hCluster.getConf(), SIZE, SIZE);
      }

      protected void tearDown() {
        try {
          closeTest();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    return setup;
  }

  public static void closeTest() throws IOException {
    m1.close();
    m2.close();
  }

  public void testSparsity() throws IOException {
    boolean appeared = false;
    for(int i = 0; i < m1.getRows(); i++) {
      for(int j = 0; j < m1.getColumns(); j++) {
        if(m1.get(i, j) == 0)
          appeared = true;
      }
    }
    
    assertTrue(appeared);
  }
  
  /**
   * Test matrices multiplication
   * 
   * @throws IOException
   */
  public void testMatrixMult() throws IOException {
    SparseMatrix result = m1.mult(m2);
    verifyMultResult(m1, m2, result);
  }
  
  /**
   * Verifying multiplication result
   * 
   * @param m1
   * @param m2
   * @param result
   * @throws IOException
   */
  private void verifyMultResult(SparseMatrix m1, SparseMatrix m2, SparseMatrix result)
      throws IOException {
    double[][] c = new double[SIZE][SIZE];

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        for (int k = 0; k < SIZE; k++) {
          c[i][k] += m1.get(i, j) * m2.get(j, k);
        }
      }
    }

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        double gap = (c[i][j] - result.get(i, j));
        assertTrue(gap < 0.000001 || gap < -0.000001);
      }
    }
  }
}
