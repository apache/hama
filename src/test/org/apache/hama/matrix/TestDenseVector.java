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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaCluster;
import org.apache.hama.io.DoubleEntry;

public class TestDenseVector extends HamaCluster {
  final static Log LOG = LogFactory.getLog(TestDenseVector.class.getName());

  private final double cosine = 0.6978227007909176;
  private final double norm1 = 12.0;
  private final double norm2 = 6.782329983125268;
  private final double normInf = 5.0;
  private final double norm2Robust = 6.782329983125269;
  private double[][] values = { { 2, 5, 1, 4 }, { 4, 1, 3, 3 } };
  private DenseMatrix m1;
  private DenseVector v1;
  private DenseVector v2;
  private DenseVector smallSize = new DenseVector();

  /**
   * @throws UnsupportedEncodingException
   */
  public TestDenseVector() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    m1 = new DenseMatrix(getConf(), 2, 4);

    for (int i = 0; i < 2; i++)
      for (int j = 0; j < 4; j++)
        m1.set(i, j, values[i][j]);

    v1 = m1.getRow(0);
    v2 = m1.getRow(1);
    smallSize.set(0, 0.5);
  }

  /**
   * @throws IOException 
   */
  public void testDenseVector() throws IOException {
    double cos = v1.dot(v2);
    assertEquals(cos, cosine);

    boolean except = false;
    try {
      v1.dot(smallSize);
    } catch (IndexOutOfBoundsException e) {
      except = true;
    }
    
    assertTrue(except);
    subVector();
        
    assertEquals(norm1, v1.norm(Vector.Norm.One));
    assertEquals(norm2, v1.norm(Vector.Norm.Two));
    assertEquals(normInf, v1.norm(Vector.Norm.Infinity));
    assertEquals(norm2Robust, v1.norm(Vector.Norm.TwoRobust));

    getSetTest();
    add();
    scalingTest();
    setTest();
    clear();
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
  public void getSetTest() throws IOException {
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
  public void add() {
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
  
  public void setTest() {
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
  
  public void subVector() {
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
   * Clear test
   */
  public void clear() {
    ((DenseVector) v1).clear();
    assertEquals(v1.size(), 0);
  }
}
