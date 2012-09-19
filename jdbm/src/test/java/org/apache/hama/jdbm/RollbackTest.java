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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RollbackTest extends TestCaseWithTestFile {

  public void test_treemap() throws IOException {
    DB db = newDBCache();
    Map<Integer, String> map = db.createTreeMap("collectionName");

    map.put(1, "one");
    map.put(2, "two");

    assertEquals(2, map.size());
    db.commit(); // persist changes into disk

    map.put(3, "three");
    assertEquals(3, map.size());
    db.rollback(); // revert recent changes
    assertEquals(2, map.size());
  }

  public void test_hashmap() throws IOException {
    DB db = newDBCache();
    Map<Integer, String> map = db.createHashMap("collectionName");

    map.put(1, "one");
    map.put(2, "two");

    assertEquals(2, map.size());
    db.commit(); // persist changes into disk

    map.put(3, "three");
    assertEquals(3, map.size());
    db.rollback(); // revert recent changes
    assertEquals(2, map.size());
  }

  public void test_treeset() throws IOException {
    DB db = newDBCache();
    Set<Integer> c = db.createTreeSet("collectionName");

    c.add(1);
    c.add(2);

    assertEquals(2, c.size());
    db.commit(); // persist changes into disk

    c.add(3);
    assertEquals(3, c.size());
    db.rollback(); // revert recent changes
    assertEquals(2, c.size());
  }

  public void test_hashset() throws IOException {
    DB db = newDBCache();
    Set<Integer> c = db.createHashSet("collectionName");

    c.add(1);
    c.add(2);

    assertEquals(2, c.size());
    db.commit(); // persist changes into disk

    c.add(3);
    assertEquals(3, c.size());
    db.rollback(); // revert recent changes
    assertEquals(2, c.size());
  }

  public void test_linkedlist() throws IOException {
    DB db = newDBCache();
    List<Integer> c = db.createLinkedList("collectionName");

    c.add(1);
    c.add(2);

    assertEquals(2, c.size());
    db.commit(); // persist changes into disk

    c.add(3);
    assertEquals(3, c.size());
    db.rollback(); // revert recent changes
    assertEquals(2, c.size());
  }

}
