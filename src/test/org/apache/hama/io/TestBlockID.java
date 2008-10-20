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
package org.apache.hama.io;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestBlockID extends TestCase {
  final static Log LOG = LogFactory.getLog(TestBlockID.class.getName());

  /**
   * BlockID object compare
   */
  public void testCompare() {
    BlockID a = new BlockID(1, 3);
    BlockID b = new BlockID(1, 1);
    assertEquals(a.compareTo(b), 1);

    BlockID c = new BlockID(3, 1);
    BlockID d = new BlockID(1, 1);
    assertEquals(a.compareTo(c), -1);

    assertEquals(b.compareTo(d), 0);
  }

}
