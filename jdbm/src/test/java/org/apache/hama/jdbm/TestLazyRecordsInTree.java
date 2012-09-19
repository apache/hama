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

import java.io.IOException;
import java.util.Map;

public class TestLazyRecordsInTree extends TestCaseWithTestFile {

  String makeString(int size) {
    StringBuilder s = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      s.append('a');
    }
    return s.toString();
  }

  void doIt(DBStore r, Map<Integer, String> m) throws IOException {
    m.put(1, "");
    long counter = r.countRecords();
    // number of records should increase after inserting big record
    m.put(1, makeString(1000));
    assertEquals(counter + 1, r.countRecords());
    assertEquals(m.get(1), makeString(1000));

    // old record should be disposed when replaced with big record
    m.put(1, makeString(1001));
    assertEquals(counter + 1, r.countRecords());
    assertEquals(m.get(1), makeString(1001));

    // old record should be disposed when replaced with small record
    m.put(1, "aa");
    assertEquals(counter, r.countRecords());
    assertEquals(m.get(1), "aa");

    // old record should be disposed after deleting
    m.put(1, makeString(1001));
    assertEquals(counter + 1, r.countRecords());
    assertEquals(m.get(1), makeString(1001));
    m.remove(1);
    assertTrue(counter >= r.countRecords());
    assertEquals(m.get(1), null);

  }

  public void testBTree() throws IOException {
    DBStore r = newDBNoCache();
    Map<Integer, String> m = r.createTreeMap("test");
    doIt(r, m);
  }

  public void testHTree() throws IOException {
    DBStore r = newDBNoCache();
    Map<Integer, String> m = r.createHashMap("test");
    doIt(r, m);
  }

}
