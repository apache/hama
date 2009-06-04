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

import org.apache.log4j.Logger;

import junit.framework.TestCase;

/**
 * Random variable generation test
 */
public class TestRandomVariable extends TestCase {
  static final Logger LOG = Logger.getLogger(TestRandomVariable.class);
  final static int COUNT = 50;

  /**
   * Random object test
   * 
   * @throws Exception
   */
  public void testRand() throws Exception {
    for (int i = 0; i < COUNT; i++) {
      double result = RandomVariable.rand();
      assertTrue(result >= 0.0d && result <= 1.0);
    }
    
    int nonZero = 0;
    for (int i = 0; i < COUNT; i++) {
      if(RandomVariable.rand(70) > 0)
        nonZero++;
    }
    assertTrue((COUNT/2) < nonZero);
  }

  /**
   * Random integer test
   * 
   * @throws Exception
   */
  public void testRandInt() throws Exception {
    final int min = 122;
    final int max = 561;

    for (int i = 0; i < COUNT; i++) {
      int result = RandomVariable.randInt(min, max);
      assertTrue(result >= min && result <= max);
    }
  }

  /**
   * Uniform test
   * 
   * @throws Exception
   */
  public void testUniform() throws Exception {
    final double min = 1.0d;
    final double max = 3.0d;

    for (int i = 0; i < COUNT; i++) {
      double result = RandomVariable.uniform(min, max);
      assertTrue(result >= min && result <= max);
    }
  }
}
