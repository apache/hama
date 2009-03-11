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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hama.util.BytesUtil;

public class TestSparseVector extends TestCase {
  final static Log LOG = LogFactory.getLog(TestSparseVector.class.getName());
  private static SparseMatrix m1;
  private static SparseVector v1;
  private static SparseVector v2;
  private static double[][] values = { { 2, 0, 0, 4 }, { 0, 0, 3, 3 } };

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(TestSparseVector.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        m1 = new SparseMatrix(hCluster.getConf());

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
   * Test get/set methods
   * 
   * @throws IOException
   */
  public void testGetSet() throws IOException {
    assertEquals(v1.get(1), 0.0);
    assertEquals(v2.get(1), 0.0);

    HTable table = m1.getHTable();
    Cell c = table.get(BytesUtil.getRowIndex(0), BytesUtil.getColumnIndex(1));
    assertTrue(c == null);
  }
  
  /**
   * Test add()
   */
  public void testAdd() {
    v1.add(v2);
    
    for(int i = 0; i < values[0].length; i++) {
      assertEquals(v1.get(i), values[0][i] + values[1][i]);
    }

    v1.add(0.5, v2);
    for(int i = 0; i < values[0].length; i++) {
      assertEquals(v1.get(i),  (values[0][i] + values[1][i]) + (0.5 * values[1][i]));
    }
  }
}
