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
import java.util.Iterator;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hama.io.VectorEntry;
import org.apache.log4j.Logger;

/**
 * Matrix test
 */
public class TestDenseMatrix extends TestCase {
  static final Logger LOG = Logger.getLogger(TestDenseMatrix.class);
  private static int SIZE = 10;
  private static Matrix m1;
  private static Matrix m2;
  private final static String SAVE = "save";
  
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestDenseMatrix.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        m1 = DenseMatrix.random(hCluster.getConf(), SIZE, SIZE);
        m2 = DenseMatrix.random(hCluster.getConf(), SIZE, SIZE);
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

  /**
   * Column vector test.
   * 
   * @param rand
   * @throws IOException
   */
  public void testGetColumn() throws IOException {
    Vector v = m1.getColumn(0);
    Iterator<VectorEntry> it = v.iterator();
    int x = 0;
    while (it.hasNext()) {
      assertEquals(m1.get(x, 0), it.next().getValue());
      x++;
    }
  }

  public void testGetSetAttribute() throws IOException {
    m1.setRowAttribute(0, "row1");
    assertEquals(m1.getRowAttribute(0), "row1");
    assertEquals(m1.getRowAttribute(1), null);

    m1.setColumnAttribute(0, "column1");
    assertEquals(m1.getColumnAttribute(0), "column1");
    assertEquals(m1.getColumnAttribute(1), null);
  }

  public void testSubMatrix() throws IOException {
    SubMatrix a = m1.subMatrix(2, 4, 2, 4);
    for (int i = 0; i < a.size(); i++) {
      for (int j = 0; j < a.size(); j++) {
        assertEquals(a.get(i, j), m1.get(i + 2, j + 2));
      }
    }
    
    SubMatrix b = m2.subMatrix(0, 2, 0, 2);
    SubMatrix c = a.mult(b);
    
    double[][] C = new double[3][3];
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 3; k++) {
          C[i][k] += m1.get(i + 2, j + 2) * m2.get(j, k);
        }
      }
    }
    
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        assertEquals(C[i][j], c.get(i, j));
      }
    }
  }

  /**
   * Test matrices addition
   * 
   * @throws IOException
   */
  public void testMatrixAdd() throws IOException {
    Matrix result = m1.add(m2);

    assertEquals(result.getRows(), SIZE);
    assertEquals(result.getColumns(), SIZE);

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertEquals(result.get(i, j), m1.get(i, j) + m2.get(i, j));
      }
    }
  }

  /**
   * Test matrices multiplication
   * 
   * @throws IOException
   */
  public void testMatrixMult() throws IOException {
    Matrix result = m1.mult(m2);

    assertEquals(result.getRows(), SIZE);
    assertEquals(result.getColumns(), SIZE);

    verifyMultResult(m1, m2, result);
  }

  public void testSetRow() throws IOException {
    Vector v = new DenseVector();
    double[] entries = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

    for (int i = 0; i < SIZE; i++) {
      v.set(i, entries[i]);
    }

    m1.setRow(SIZE + 1, v);
    Iterator<VectorEntry> it = m1.getRow(SIZE + 1).iterator();

    // We should remove the timestamp and row attribute from the vector
    int i = 0;
    while (it.hasNext()) {
      assertEquals(entries[i], it.next().getValue());
      i++;
    }
  }

  public void testLoadSave() throws IOException {
    m1.save(SAVE);
    HCluster hCluster = new HCluster();
    DenseMatrix loadTest = new DenseMatrix(hCluster.conf, SAVE);

    for (int i = 0; i < loadTest.getRows(); i++) {
      for (int j = 0; j < loadTest.getColumns(); j++) {
        assertEquals(m1.get(i, j), loadTest.get(i, j));
      }
    }
    
    assertEquals(loadTest.getName(), SAVE);
  }

  /**
   * Verifying multiplication result
   * 
   * @param m1
   * @param m2
   * @param result
   * @throws IOException
   */
  private void verifyMultResult(Matrix m1, Matrix m2, Matrix result)
      throws IOException {
    double[][] C = new double[SIZE][SIZE];

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        for (int k = 0; k < SIZE; k++) {
          C[i][k] += m1.get(i, j) * m2.get(j, k);
        }
      }
    }

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertEquals(String.valueOf(result.get(i, j)).substring(0, 14), String
            .valueOf(C[i][j]).substring(0, 14));
      }
    }
  }
}
