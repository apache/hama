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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.RandomVariable;
import org.apache.log4j.Logger;

/**
 * Matrix test
 */
public class TestDenseMatrix extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestDenseMatrix.class);
  private int SIZE = 10;
  private Matrix m1;
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
  }

  public void testAddMult() throws IOException {
    double origin = m1.get(1, 1);
    m1.add(1, 1, 0.5);
    assertEquals(m1.get(1, 1), origin + 0.5);
    
    getRowColumnVector();
    setRowColumnVector();

    setMatrix(m1);
    setAlphaMatrix(m1);
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
      assertEquals(m1.get(x, 0), ((DoubleWritable) it.next()).get());
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
      assertEquals(entries[i], ((DoubleWritable) it.next()).get());
      i++;
    }

    m1.setColumn(SIZE, v);
    Iterator<Writable> it2 = m1.getColumn(SIZE).iterator();

    int x = 0;
    while (it2.hasNext()) {
      assertEquals(entries[x], ((DoubleWritable) it2.next()).get());
      x++;
    }
  }
}
