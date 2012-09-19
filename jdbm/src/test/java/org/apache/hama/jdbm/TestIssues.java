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

public class TestIssues extends TestCaseWithTestFile {

  /*
   * test this issue http://code.google.com/p/jdbm2/issues/detail?id=2
   */
  public void testHTreeClear() throws IOException {
    final DBAbstract db = newDBCache();
    final HTree<String, String> tree = (HTree) db.createHashMap("name");

    for (int i = 0; i < 1001; i++) {
      tree.put(String.valueOf(i), String.valueOf(i));
    }
    db.commit();
    System.out.println("finished adding");

    tree.clear();
    db.commit();
    System.out.println("finished clearing");
    assertTrue(tree.isEmpty());
  }

  public void testBTreeClear() throws IOException {
    final DB db = newDBCache();
    final Map<String, String> treeMap = db.createTreeMap("test");

    for (int i = 0; i < 1001; i++) {
      treeMap.put(String.valueOf(i), String.valueOf(i));
    }
    db.commit();
    System.out.println("finished adding");

    treeMap.clear();
    db.commit();
    System.out.println("finished clearing");
    assertTrue(treeMap.isEmpty());
  }

  public void test_issue_84_reopen_after_close() {
    String f = newTestFile();
    DB db = DBMaker.openFile(f).make();
    db.close();

    db = DBMaker.openFile(f).readonly().make();
    db.close();
  }
}
