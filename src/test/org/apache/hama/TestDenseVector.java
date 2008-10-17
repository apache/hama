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

import java.util.Iterator;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.io.VectorEntry;

public class TestDenseVector extends TestCase {
  final static Log LOG = LogFactory.getLog(TestDenseVector.class.getName());
  
  private static final double cosine = 0.6978227007909176;
  private static final double norm1 = 12.0;
  private static final double norm2 = 6.782329983125268;
  private static double[][] values = { { 2, 5, 1, 4 }, { 4, 1, 3, 3 } };
  private static Matrix m1;
  private static Vector v1;
  private static Vector v2;

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestDenseVector.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        m1 = new DenseMatrix(hCluster.getConf(), "vectorTest");

        for (int i = 0; i < 2; i++)
          for (int j = 0; j < 4; j++)
            m1.set(i, j, values[i][j]);

        v1 = m1.getRow(0);
        v2 = m1.getRow(1);
      }

      protected void tearDown() {
        LOG.info("tearDown()");
      }
    };
    return setup;
  }

  /**
   * Test |a| dot |b|
   */
  public void testDot() {
    double cos = v1.dot(v2);
    assertEquals(cos, cosine);
  }

  public void testSubVector() {
    int start = 2;
    Vector subVector = v1.subVector(start, v1.size() - 1);
    Iterator<VectorEntry> it = subVector.iterator();

    int i = start;
    while (it.hasNext()) {
      assertEquals(v1.get(i), it.next().getValue());
      i++;
    }
  }

  /**
   * Test norm one
   */
  public void testNom1() {
    double result = ((DenseVector) v1).getNorm1();
    assertEquals(norm1, result);
  }

  /**
   * Test norm two
   */
  public void testNom2() {
    double result = ((DenseVector) v1).getNorm2();
    assertEquals(norm2, result);
  }

  /**
   * Test scaling
   */
  public void scalingTest() {
    v2.scale(0.5);

    for (int i = 0; i < v2.size(); i++) {
      assertEquals(values[1][i] * 0.5, v2.get(i));
    }
  }

  /**
   * Test get/set methods
   */
  public void testGetSet() {
    assertEquals(v1.get(0), values[0][0]);
  }

  /**
   * Test add()
   */
  public void testAdd() {
    v1.add(v2);
    int i = 0;
    Iterator<VectorEntry> it = v1.iterator();
    while (it.hasNext()) {
      VectorEntry c = it.next();
      assertEquals(c.getValue(), values[0][i] + values[1][i]);
      i++;
    }

    v1.add(0.5, v2);
    int j = 0;
    Iterator<VectorEntry> itt = v1.iterator();
    while (itt.hasNext()) {
      VectorEntry c = itt.next();
      assertEquals(c.getValue(), (values[0][j] + values[1][j]) + (0.5 * values[1][j]));
      j++;
    }
    
    double old = v1.get(0);
    v1.add(0, norm1);
    assertEquals(v1.get(0), old + norm1);
  }
}
