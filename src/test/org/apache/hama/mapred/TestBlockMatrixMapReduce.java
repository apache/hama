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
package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hama.DenseMatrix;
import org.apache.hama.HCluster;
import org.apache.log4j.Logger;

public class TestBlockMatrixMapReduce extends HCluster {
  static final Logger LOG = Logger.getLogger(TestBlockMatrixMapReduce.class);
  static final int SIZE = 32;

  /** constructor */
  public TestBlockMatrixMapReduce() {
    super();
  }

  public void testBlockMatrixMapReduce() throws IOException,
      ClassNotFoundException {
    DenseMatrix m1 = DenseMatrix.random(conf, SIZE, SIZE);
    DenseMatrix m2 = DenseMatrix.random(conf, SIZE, SIZE);

    DenseMatrix c = (DenseMatrix) m1.mult(m2, 16);

    double[][] mem = new double[SIZE][SIZE];
    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        for (int k = 0; k < SIZE; k++) {
          mem[i][k] += m1.get(i, j) * m2.get(j, k);
        }
      }
    }

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        double gap = (mem[i][j] - c.get(i, j));
        assertTrue(gap < 0.000001 || gap < -0.000001);
      }
    }
  }
}
