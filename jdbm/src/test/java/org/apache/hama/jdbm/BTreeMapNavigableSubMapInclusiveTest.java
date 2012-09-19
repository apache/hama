/**
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
package org.apache.hama.jdbm;

public class BTreeMapNavigableSubMapInclusiveTest extends
    BTreeMapNavigable2Test {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    map.put(0, "zero");
    map.put(11, "eleven");
    map = map.subMap(1, true, 10, true);
  }

  @Override
  public void testPut() {
    // this test is not run on submaps
  }
}
