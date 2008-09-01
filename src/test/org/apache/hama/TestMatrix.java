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

/**
 * Matrix test
 */
public class TestMatrix extends HamaTestCase {

  /**
   * Random matrix creation test
   * 
   * @throws IOException
   * 
   * @throws IOException
   */
  public void testRandomMatrix() throws IOException {
    Matrix rand = DenseMatrix.random(conf, SIZE, SIZE);
    assertTrue(rand.getRows() == SIZE);
    
    getColumnTest(rand);
  }

  /**
   * Column vector test.
   * 
   * @param rand
   * @throws IOException
   */
  public void getColumnTest(Matrix rand) throws IOException {
    Vector v = rand.getColumn(0);
    Iterator<Cell> it = v.iterator();
    int x = 0;
    while (it.hasNext()) {
      assertEquals(rand.get(x, 0), Numeric.bytesToDouble(it.next().getValue()));
      x++;
    }
  }
}
