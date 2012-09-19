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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DefragTest extends TestCaseWithTestFile {

  public void testDefrag1() throws IOException {
    String file = newTestFile();
    DBStore m = new DBStore(file, false, false, false);
    long loc = m.insert("123");
    m.defrag(true);
    m.close();
    m = new DBStore(file, false, false, false);
    assertEquals(m.fetch(loc), "123");
  }

  public void testDefrag2() throws IOException {
    String file = newTestFile();
    DBStore m = new DBStore(file, false, false, false);
    TreeMap<Long, String> map = new TreeMap<Long, String>();
    for (int i = 0; i < 10000; i++) {
      long loc = m.insert("" + i);
      map.put(loc, "" + i);
    }

    m.defrag(true);
    m.close();
    m = new DBStore(file, false, false, false);
    for (Long l : map.keySet()) {
      String val = map.get(l);
      assertEquals(val, m.fetch(l));
    }
  }

  public void testDefragBtree() throws IOException {
    String file = newTestFile();
    DBStore m = new DBStore(file, false, false, false);
    Map t = m.createTreeMap("aa");
    TreeMap t2 = new TreeMap();
    for (int i = 0; i < 10000; i++) {
      t.put(i, "" + i);
      t2.put(i, "" + i);
    }

    m.defrag(true);
    m.close();
    m = new DBStore(file, false, false, false);
    t = m.getTreeMap("aa");
    assertEquals(t, t2);
  }

  public void testDefragLinkedList() throws Exception {
    String file = newTestFile();
    DBStore r = new DBStore(file, false, false, false);
    List l = r.createLinkedList("test");
    Map<Long, Double> junk = new LinkedHashMap<Long, Double>();

    for (int i = 0; i < 1e4; i++) {
      // insert some junk
      Double d = Math.random();
      l.add(d);
      junk.put(r.insert(d), d);
    }
    r.commit();
    // make copy of linked list
    List l2 = new ArrayList(l);
    long oldRecCount = r.countRecords();
    r.defrag(true);

    r.close();
    r = new DBStore(file, false, false, false);
    assertEquals(oldRecCount, r.countRecords());

    // compare that list was unchanged
    assertEquals(l2, new ArrayList(r.getLinkedList("test")));

    // and check that random junk still have the same recids
    for (Long recid : junk.keySet()) {
      assertEquals(junk.get(recid), r.fetch(recid));
    }

    r.close();
  }
}
