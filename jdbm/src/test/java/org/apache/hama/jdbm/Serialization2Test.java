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
import java.io.Serializable;
import java.util.Map;

public class Serialization2Test extends TestCaseWithTestFile {

  public void test2() throws IOException {
    DB db = newDBNoCache();

    Serialization2Bean processView = new Serialization2Bean();

    Map<Object, Object> map = db.createHashMap("test2");

    map.put("abc", processView);

    db.commit();

    Serialization2Bean retProcessView = (Serialization2Bean) map.get("abc");
    assertEquals(processView, retProcessView);

    db.close();
  }

  public void test3() throws IOException {

    String file = newTestFile();

    Serialized2DerivedBean att = new Serialized2DerivedBean();
    DB db = DBMaker.openFile(file).disableCache().make();

    Map<Object, Object> map = db.createHashMap("test");

    map.put("att", att);
    db.commit();
    db.close();
    db = DBMaker.openFile(file).disableCache().make();
    map = db.getHashMap("test");

    Serialized2DerivedBean retAtt = (Serialized2DerivedBean) map.get("att");
    assertEquals(att, retAtt);
  }

  static class AAA implements Serializable {
    String test = "aa";
  }

  public void testReopenWithDefrag() {

    String f = newTestFile();

    DB db = DBMaker.openFile(f).disableTransactions().make();

    Map<Integer, AAA> map = db.createTreeMap("test");
    map.put(1, new AAA());

    db.defrag(true);
    db.close();

    db = DBMaker.openFile(f).disableTransactions().make();

    map = db.getTreeMap("test");
    assertNotNull(map.get(1));
    assertEquals(map.get(1).test, "aa");

    db.close();
  }

}
