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

import java.util.NavigableMap;
import java.util.Set;

public class DBCacheTest extends TestCaseWithTestFile {

  // https://github.com/jankotek/JDBM3/issues/11
  public void test_Issue_11_soft_cache_record_disappear() {
    long MAX = (long) 1e6;

    String file = newTestFile();
    DB d = DBMaker.openFile(file).disableTransactions().enableSoftCache()
        .make();

    Set<Integer> set = d.createHashSet("1");

    for (Integer i = 0; i < MAX; i++) {
      set.add(i);
    }

    d.close();

    d = DBMaker.openFile(file).disableTransactions().enableSoftCache().make();

    set = d.getHashSet("1");
    for (Integer i = 0; i < MAX; i++) {
      assertTrue(set.contains(i));
    }

  }

  public void test_issue_xyz() {
    org.apache.hama.jdbm.DB db = DBMaker.openFile(newTestFile()).enableSoftCache()
        .make();
    NavigableMap<String,String> m = db.createTreeMap("test");

    for (int i = 0; i < 1e5; i++) {
      m.put("test" + i, "test" + i);
    }
    db.close();
    //
    // problem in cache, throws;
    // java.lang.IllegalArgumentException: Argument 'recid' is invalid: 0
    // at org.apache.jdbm.DBStore.fetch(DBStore.java:356)
    // at org.apache.jdbm.DBCache.fetch(DBCache.java:292)
    // at org.apache.jdbm.BTreeNode.loadNode(BTreeNode.java:833)
    // at org.apache.jdbm.BTreeNode.insert(BTreeNode.java:391)
    // at org.apache.jdbm.BTreeNode.insert(BTreeNode.java:392)
    // at org.apache.jdbm.BTreeNode.insert(BTreeNode.java:392)
    // at org.apache.jdbm.BTree.insert(BTree.java:281)
    // at org.apache.jdbm.BTreeMap.put(BTreeMap.java:285)
    // at org.apache.jdbm.DBCacheTest.test_some_random_shit(DBCacheTest.java:48)
    //

  }
}
