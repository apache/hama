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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hama.HamaConfiguration;
import org.apache.log4j.Logger;

public class TestMatrixVectorMult extends TestCase {
  static final Logger LOG = Logger.getLogger(TestMatrixVectorMult.class);
  private static Matrix m1;
  private static Matrix m2;
  private static HamaConfiguration conf;
  private static double[][] result = { { 5 }, { 11 } };

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestMatrixVectorMult.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        conf = hCluster.getConf();

        m1 = new DenseMatrix(conf, "A", true);
        m1.setDimension(2, 2);
        m1.set(0, 0, 1);
        m1.set(0, 1, 2);
        m1.set(1, 0, 3);
        m1.set(1, 1, 4);
        m2 = new DenseMatrix(conf, "B", true);
        m2.setDimension(2, 1);
        m2.set(0, 0, 1);
        m2.set(1, 0, 2);
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

  public void testMatVectorMult() throws IOException {
    DenseMatrix c = (DenseMatrix) m1.mult(m2);
    assertTrue(m1.getRows() == 2);

    for (int i = 0; i < c.getRows(); i++) {
      for (int j = 0; j < c.getColumns(); j++) {
        assertEquals(result[i][j], c.get(i, j));
      }
    }
  }
}
