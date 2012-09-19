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

public class CompactTest extends TestCaseWithTestFile {

  final int MAX = 1000 * 1000;

  public void testHashCompaction() throws IOException {

    String f = newTestFile();

    DB db0 = DBMaker.openFile(f).disableTransactions().make();
    Map<String, String> db = db0.createHashMap("db");

    System.out.println("Adding");
    for (int i = 0; i < MAX; i++) {
      db.put("key" + i, "value" + i);
    }

    db0.close();
    db0 = DBMaker.openFile(f).disableTransactions().make();
    db = db0.getHashMap("db");

    System.out.println("Deleting");
    for (int i = 0; i < MAX; i++) {
      db.remove("key" + i);
    }

    db0.close();
    db0 = DBMaker.openFile(f).disableTransactions().make();
    db = db0.getHashMap("db");

    System.out.println("Adding");
    for (int i = 0; i < MAX; i++) {
      db.put("key" + i, "value" + i);
    }
    System.out.println("Closing");
    db0.close();
  }

  public void testBTreeCompaction() throws IOException {

    String f = newTestFile();

    DB db0 = DBMaker.openFile(f).disableTransactions().make();
    Map<String, String> db = db0.createTreeMap("db");

    System.out.println("Adding");
    for (int i = 0; i < MAX; i++) {
      db.put("key" + i, "value" + i);
    }

    db0.close();
    db0 = DBMaker.openFile(f).disableTransactions().make();
    db = db0.getTreeMap("db");

    System.out.println("Deleting");
    for (int i = 0; i < MAX; i++) {
      db.remove("key" + i);
    }

    db0.close();
    db0 = DBMaker.openFile(f).disableTransactions().make();
    db = db0.getTreeMap("db");

    System.out.println("Adding");
    for (int i = 0; i < MAX; i++) {
      db.put("key" + i, "value" + i);
    }

    System.out.println("Closing");
    db0.close();
  }

}
