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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.io.DoubleEntry;

public class TestDenseVector extends TestCase {
  final static Log LOG = LogFactory.getLog(TestDenseVector.class.getName());
  
  private static final double cosine = 0.6978227007909176;
  private static final double norm1 = 12.0;
  private static final double norm2 = 6.782329983125268;
  private static final double normInf = 5.0;
  private static final double norm2Robust = 6.782329983125269;
  private static double[][] values = { { 2, 5, 1, 4 }, { 4, 1, 3, 3 } };
  private static DenseMatrix m1;
  private static DenseVector v1;
  private static DenseVector v2;
  private static DenseVector smallSize = new DenseVector();
  
  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestDenseVector.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        m1 = new DenseMatrix(hCluster.getConf(), 2, 4);

        for (int i = 0; i < 2; i++)
          for (int j = 0; j < 4; j++)
            m1.set(i, j, values[i][j]);

        v1 = m1.getRow(0);
        v2 = m1.getRow(1);
        smallSize.set(0, 0.5);
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
    
    boolean except = false;
    try {
      v1.dot(smallSize);
    } catch (IndexOutOfBoundsException e) {
      except = true;
    }
    
    assertTrue(except);
  }

  public void testSubVector() {
    int start = 2;
    Vector subVector = v1.subVector(start, v1.size() - 1);
    Iterator<Writable> it = subVector.iterator();

    int i = start;
    while (it.hasNext()) {
      assertEquals(v1.get(i), ((DoubleEntry) it.next()).getValue());
      i++;
    }
  }

  /**
   * Test norm one
   */
  public void testNom1() {
    assertEquals(norm1, v1.norm(Vector.Norm.One));
  }

  /**
   * Test norm two
   */
  public void testNom2() {
    assertEquals(norm2, v1.norm(Vector.Norm.Two));
  }

  /**
   * Test infinity norm
   */
  public void testNormInf() {
    assertEquals(normInf, v1.norm(Vector.Norm.Infinity));
  }
  
  /**
   * Test infinity norm
   */
  public void testNorm2Robust() {
    assertEquals(norm2Robust, v1.norm(Vector.Norm.TwoRobust));
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
   * @throws IOException 
   */
  public void testGetSet() throws IOException {
    assertEquals(v1.get(0), values[0][0]);
    boolean ex = false;
    try {
      v1.get(5);
    } catch (NullPointerException e) {
      ex = true;
    }
    assertTrue(ex);
    assertEquals(m1.getColumn(0).size(), 2);
  }

  /**
   * Test add()
   */
  public void testAdd() {
    v1.add(v2);
    int i = 0;
    Iterator<Writable> it = v1.iterator();
    while (it.hasNext()) {
      DoubleEntry c = (DoubleEntry) it.next();
      assertEquals(c.getValue(), values[0][i] + values[1][i]);
      i++;
    }

    v1.add(0.5, v2);
    int j = 0;
    Iterator<Writable> itt = v1.iterator();
    while (itt.hasNext()) {
      DoubleEntry c = (DoubleEntry) itt.next();
      assertEquals(c.getValue(), (values[0][j] + values[1][j]) + (0.5 * values[1][j]));
      j++;
    }
    
    double old = v1.get(0);
    v1.add(0, norm1);
    assertEquals(v1.get(0), old + norm1);
    
    boolean except = false;
    try {
      v1.add(smallSize);
    } catch (IndexOutOfBoundsException e) {
      except = true;
    }
    
    assertTrue(except);
    
    except = false;
    try {
      v1.add(0.6, smallSize);
    } catch (IndexOutOfBoundsException e) {
      except = true;
    }
    
    assertTrue(except);
  }
  
  public void testSet() {
    v1.set(v2);
    
    for(int i = 0; i < v1.size(); i ++) {
      assertEquals(v2.get(i), v1.get(i));
    }
    
    boolean except = false;
    try {
      v1.set(0.6, smallSize);
    } catch (IndexOutOfBoundsException e) {
      except = true;
    }
    
    assertTrue(except);
  }
  
  /**
   * Clear test
   */
  public void testClear() {
    ((DenseVector) v1).clear();
    assertEquals(v1.size(), 0);
  }
}
