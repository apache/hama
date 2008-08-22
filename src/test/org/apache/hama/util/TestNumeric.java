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
package org.apache.hama.util;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.Constants;

public class TestNumeric extends TestCase {
  final static int TEST_INT = 3;
  final double TEST_DOUBLE = 0.4;

  /**
   * Integer conversion test
   */
  public void testInteger() {
    assertEquals(Numeric.bytesToInt(Numeric.intToBytes(TEST_INT)), TEST_INT);
  }

  /**
   * Double conversion test
   */
  public void testDouble() {
    assertEquals(Numeric.bytesToDouble(Numeric.doubleToBytes(TEST_DOUBLE)),
        TEST_DOUBLE);
  }

  /**
   * Get the column index from hbase.
   */
  public void testGetColumnIndex() {
    byte[] result = Numeric.getColumnIndex(3);
    assertEquals(Bytes.toString(result), Constants.COLUMN
        + Numeric.getColumnIndex(result));
  }
}
