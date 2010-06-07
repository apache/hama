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

import org.apache.hama.HamaCluster;
import org.apache.log4j.Logger;

public class TestSparseMatrix extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestSparseMatrix.class);
  private int SIZE = 10;
  private SparseMatrix m1;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestSparseMatrix() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();
    m1 = SparseMatrix.random(getConf(), SIZE, SIZE);
  }

  public void testMult() throws IOException {
    assertTrue(m1.getRows() > 0);
    sparsity();
    m1.set(0, 0, -8);
    assertEquals(m1.get(0, 0), -8.0);

    SparseVector vector = new SparseVector();
    vector.set(0, 3);
    vector.set(1, -8);
    m1.setRow(0, vector);
    assertEquals(m1.get(0, 0), 3.0);
    assertEquals(m1.get(0, 1), -8.0);
    SparseVector vector2 = m1.getRow(0);
    assertEquals(vector2.get(0), 3.0);
    assertEquals(vector2.get(1), -8.0);
  }

  public void sparsity() throws IOException {
    boolean appeared = false;
    for (int i = 0; i < m1.getRows(); i++) {
      for (int j = 0; j < m1.getColumns(); j++) {
        if (m1.get(i, j) == 0)
          appeared = true;
      }
    }

    assertTrue(appeared);
  }
}
