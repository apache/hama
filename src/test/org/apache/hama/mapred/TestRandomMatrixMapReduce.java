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
import org.apache.hama.SparseMatrix;
import org.apache.log4j.Logger;

public class TestRandomMatrixMapReduce extends HCluster {
  static final Logger LOG = Logger.getLogger(TestRandomMatrixMapReduce.class);
  
  public void testRandomMatrixMapReduce() throws IOException {
    DenseMatrix rand = DenseMatrix.random_mapred(conf, 20, 20);
    assertEquals(20, rand.getRows());
    assertEquals(20, rand.getColumns());
    
    for(int i = 0; i < 20; i++) {
      for(int j = 0; j < 20; j++) {
        assertTrue(rand.get(i, j) > -1);
      }
    }
    
    rand.close();
    
    SparseMatrix rand2 = SparseMatrix.random_mapred(conf, 20, 20, 30);
    assertEquals(20, rand2.getRows());
    assertEquals(20, rand2.getColumns());
    boolean zeroAppear = false;
    for(int i = 0; i < 20; i++) {
      for(int j = 0; j < 20; j++) {
        if(rand2.get(i, j) == 0.0)
          zeroAppear = true;
      }
    }
    assertTrue(zeroAppear);
    rand2.close();
  }
}
