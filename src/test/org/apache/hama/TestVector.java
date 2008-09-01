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

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hama.util.Numeric;

public class TestVector extends HamaTestCase {
  private static final double cosine = 0.6978227007909176;
  private static final double norm1 = 12.0;
  private static final double norm2 = 6.782329983125268;
  private double[][] values = { { 2, 5, 1, 4 }, { 4, 1, 3, 3 } };
  private static final String m = "dotTest";

  /**
   * Test vector
   * @throws IOException 
   */
  public void testGetVector() throws IOException {
    Matrix m1 = new DenseMatrix(conf, m);

    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 4; j++) {
        m1.set(i, j, values[i][j]);
      }
    }

    Vector v1 = m1.getRow(0);
    Vector v2 = m1.getRow(1);

    dotTest(v1, v2);
    norm1Test(v1, v2);
    norm2Test(v1, v2);
    scalingTest(v2);
  }

  /**
   * Test |a| dot |b|
   * 
   * @param v1
   * @param v2
   */
  private void dotTest(Vector v1, Vector v2) {
    double cos = v1.dot(v2);
    assertEquals(cos, cosine);
  }

  /**
   * Test Norm one
   * 
   * @param v1
   * @param v2
   */
  private void norm1Test(Vector v1, Vector v2) {
    assertEquals(norm1, ((DenseVector) v1).getNorm1());
  }

  private void norm2Test(Vector v1, Vector v2) {
    assertEquals(norm2, ((DenseVector) v1).getNorm2());
  }

  private void scalingTest(Vector v2) {
    v2.scale(0.5);

    for (int i = 0; i < v2.size(); i++) {
      assertEquals(values[1][i] * 0.5, v2.get(i));
    }
  }

  public void testGetSet() {
    Vector v1 = new DenseVector();
    v1.set(0, 0.2);
    assertEquals(v1.get(0), 0.2);
  }

  public void testIterator() {
    Vector v1 = new DenseVector();
    v1.set(0, 0.2);
    v1.set(1, 0.5);
    double[] result = { 0.2, 0.5 };
    int i = 0;
    Iterator<Cell> it = v1.iterator();
    while (it.hasNext()) {
      Cell c = it.next();
      assertEquals(Numeric.bytesToDouble(c.getValue()), result[i]);
      i++;
    }
  }
}
