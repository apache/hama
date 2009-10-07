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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hama.HamaCluster;
import org.apache.hama.util.BytesUtil;

public class TestSparseVector extends HamaCluster {
  final static Log LOG = LogFactory.getLog(TestSparseVector.class.getName());
  private SparseMatrix m1;
  private SparseVector v1;
  private SparseVector v2;
  private double[][] values = { { 2, 0, 0, 4 }, { 0, 0, 3, 3 } };

  /**
   * @throws UnsupportedEncodingException
   */
  public TestSparseVector() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();
    m1 = new SparseMatrix(getConf(), 2, 4);

    for (int i = 0; i < 2; i++)
      for (int j = 0; j < 4; j++)
        m1.set(i, j, values[i][j]);

    v1 = m1.getRow(0);
    v2 = m1.getRow(1);
  }

  /**
   * Test get/set methods
   * 
   * @throws IOException
   */
  public void testGetSet() throws IOException {
    assertEquals(v1.get(1), 0.0);
    assertEquals(v2.get(1), 0.0);

    HTable table = m1.getHTable();
    Get get = new Get(BytesUtil.getRowIndex(0));
    get.addColumn(BytesUtil.getColumnIndex(1));
    Result r = table.get(get);
    assertTrue(r.getCellValue() == null);
    
    addTest();
  }

  /**
   * Test add()
   */
  public void addTest() {
    v1.add(v2);

    for (int i = 0; i < values[0].length; i++) {
      assertEquals(v1.get(i), values[0][i] + values[1][i]);
    }

    v1.add(0.5, v2);
    for (int i = 0; i < values[0].length; i++) {
      assertEquals(v1.get(i), (values[0][i] + values[1][i])
          + (0.5 * values[1][i]));
    }
  }
}
