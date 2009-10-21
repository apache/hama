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
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.util.RandomVariable;
import org.apache.log4j.Logger;

/**
 * Matrix test
 */
public class TestDenseMatrix extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestDenseMatrix.class);
  private int SIZE = 10;
  private Matrix m1;
  private Matrix m2;
  private HamaConfiguration conf;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestDenseMatrix() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();

    m1 = DenseMatrix.random(conf, SIZE, SIZE);
    m2 = DenseMatrix.random(conf, SIZE, SIZE);
  }

  public void testAddMult() throws IOException {

    Matrix m3 = DenseMatrix.random(conf, SIZE, SIZE);
    Matrix m4 = DenseMatrix.random(conf, SIZE - 2, SIZE - 2);
    try {
      m1.add(m4);
      fail("Matrix-Addition should be failed while rows and columns aren't same.");
    } catch (IOException e) {
      LOG.info(e.toString());
    }

    try {
      m1.mult(m4);
      fail("Matrix-Mult should be failed while A.columns!=B.rows.");
    } catch (IOException e) {
      LOG.info(e.toString());
    }
    
    double origin = m1.get(1, 1);
    m1.add(1, 1, 0.5);
    assertEquals(m1.get(1, 1), origin + 0.5);
    
    matrixAdd(m1, m2);
    multMatrixAdd(m1, m2, m3);
    matrixMult(m1, m2);
    addAlphaMatrix(m1, m2);

    getRowColumnVector();
    setRowColumnVector();
    setMatrix(m1);
    setAlphaMatrix(m1);
  }

  /**
   * Test matrices addition
   * 
   * @throws IOException
   */
  public void matrixAdd(Matrix m1, Matrix m2) throws IOException {
    Matrix result = m1.add(m2);

    assertEquals(result.getRows(), SIZE);
    assertEquals(result.getColumns(), SIZE);

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertEquals(result.get(i, j), m1.get(i, j) + m2.get(i, j));
      }
    }

    Matrix subtract = result.add(-1.0, m2);

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        double gap = (subtract.get(i, j) - m1.get(i, j));
        assertTrue(-0.00001 < gap && gap < 0.00001);
      }
    }
  }

  public void multMatrixAdd(Matrix m1, Matrix m2, Matrix m3) throws IOException {
    Matrix result = ((DenseMatrix) m1).add(m2, m3);

    assertEquals(result.getRows(), SIZE);
    assertEquals(result.getColumns(), SIZE);

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertEquals(result.get(i, j), m1.get(i, j) + m2.get(i, j)
            + m3.get(i, j));
      }
    }
  }

  /**
   * Test matrices multiplication
   * 
   * @throws IOException
   */
  public void matrixMult(Matrix m1, Matrix m2) throws IOException {
    Matrix result = m1.mult(m2);

    assertEquals(result.getRows(), SIZE);
    assertEquals(result.getColumns(), SIZE);

    verifyMultResult(m1, m2, result);
  }

  public void addAlphaMatrix(Matrix m1, Matrix m2) throws IOException {
    double value = m1.get(0, 0) + (m2.get(0, 0) * 0.1);
    Matrix result = m1.add(0.1, m2);
    assertEquals(value, result.get(0, 0));
  }
  
  public void setMatrix(Matrix m1) throws IOException {
    Matrix a = new DenseMatrix(conf, m1.getRows(), m1.getColumns());
    a.set(m1);

    for (int i = 0; i < 5; i++) {
      // between 0 ~ SIZE -1
      int x = RandomVariable.randInt(0, SIZE - 1);
      int y = RandomVariable.randInt(0, SIZE - 1);
      assertEquals(a.get(x, y), m1.get(x, y));
    }
  }

  public void setAlphaMatrix(Matrix m1) throws IOException {
    Matrix a = new DenseMatrix(conf, m1.getRows(), m1.getColumns());
    a.set(0.5, m1);

    for (int i = 0; i < 5; i++) {
      int x = RandomVariable.randInt(0, SIZE - 1);
      int y = RandomVariable.randInt(0, SIZE - 1);
      assertEquals(a.get(x, y), (m1.get(x, y) * 0.5));
    }
  }

  public void getRowColumnVector() throws IOException {
    boolean ex = false;
    try {
      m1.get(SIZE + 1, SIZE + 1);
    } catch (ArrayIndexOutOfBoundsException e) {
      ex = true;
    }
    assertTrue(ex);
    assertTrue(m1.get(0, 0) > 0);
    
    Vector v = m1.getColumn(0);
    Iterator<Writable> it = v.iterator();
    int x = 0;
    while (it.hasNext()) {
      assertEquals(m1.get(x, 0), ((DoubleEntry) it.next()).getValue());
      x++;
    }
    
    m1.setRowLabel(0, "row1");
    assertEquals(m1.getRowLabel(0), "row1");
    assertEquals(m1.getRowLabel(1), null);

    m1.setColumnLabel(0, "column1");
    assertEquals(m1.getColumnLabel(0), "column1");
    assertEquals(m1.getColumnLabel(1), null);
  }
  
  public void setRowColumnVector() throws IOException {
    Vector v = new DenseVector();
    double[] entries = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

    for (int i = 0; i < SIZE; i++) {
      v.set(i, entries[i]);
    }

    m1.setRow(SIZE, v);
    Iterator<Writable> it = m1.getRow(SIZE).iterator();

    int i = 0;
    while (it.hasNext()) {
      assertEquals(entries[i], ((DoubleEntry) it.next()).getValue());
      i++;
    }

    m1.setColumn(SIZE, v);
    Iterator<Writable> it2 = m1.getColumn(SIZE).iterator();

    int x = 0;
    while (it2.hasNext()) {
      assertEquals(entries[x], ((DoubleEntry) it2.next()).getValue());
      x++;
    }
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
        assertTrue((Math.abs(c[i][j] - result.get(i, j)) < .0000001));
      }
    }
  }
}
