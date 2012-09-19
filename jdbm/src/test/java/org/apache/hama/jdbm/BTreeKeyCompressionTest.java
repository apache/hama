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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.SortedMap;

public class BTreeKeyCompressionTest extends TestCaseWithTestFile {

  static final long size = (long) 1e5;

  public void testExpand() throws IOException {
    long init = Long.MAX_VALUE - size * 2;
    String file = newTestFile();
    DB db = new DBStore(file, false, false, false);
    SortedMap<Long, String> map = db.createTreeMap("aa");
    for (long i = init; i < init + size; i++) {
      map.put(i, "");
    }
    db.commit();
    db.defrag(true);
    db.close();
    long fileSize = new File(file + ".dbr.0").length() / 1024;
    System.out.println("file size: " + fileSize);
    assertTrue("file is too big, compression failed", fileSize < 1000);
  }

  public void testCornersLimitsLong() throws IOException {
    DB db = newDBCache();
    SortedMap<Long, String> map = db.createTreeMap("aa");
    ArrayList<Long> ll = new ArrayList<Long>();
    for (Long i = Long.MIN_VALUE; i < Long.MIN_VALUE + 1000; i++) {
      map.put(i, "");
      ll.add(i);
    }
    for (Long i = -1000l; i < 1000; i++) {
      map.put(i, "");
      ll.add(i);
    }
    for (Long i = Long.MAX_VALUE - 1000; i <= Long.MAX_VALUE && i > 0; i++) {
      map.put(i, "");
      ll.add(i);
    }

    db.commit();

    db.clearCache();
    for (Long i : ll) {
      assertTrue("failed for " + i, map.containsKey(i));
    }

    assertTrue(!map.containsKey(Long.valueOf(Long.MIN_VALUE + 1000)));
    assertTrue(!map.containsKey(Long.valueOf(Long.MIN_VALUE + 1001)));
    assertTrue(!map.containsKey(Long.valueOf(-1001L)));
    assertTrue(!map.containsKey(Long.valueOf(-1002L)));
    assertTrue(!map.containsKey(Long.valueOf(1001L)));
    assertTrue(!map.containsKey(Long.valueOf(1002L)));
    assertTrue(!map.containsKey(Long.valueOf(Long.MAX_VALUE - 1001)));
    assertTrue(!map.containsKey(Long.valueOf(Long.MAX_VALUE - 1002)));

    db.close();
  }

  public void testCornersLimitsInt() throws IOException {
    DB db = newDBCache();
    SortedMap<Integer, String> map = db.createTreeMap("aa");
    ArrayList<Integer> ll = new ArrayList<Integer>();
    for (Integer i = Integer.MIN_VALUE; i < Integer.MIN_VALUE + 1000; i++) {
      map.put(new Integer(i), "");
      ll.add(new Integer(i));
    }
    for (Integer i = -1000; i < 1000; i++) {
      map.put(i, "");
      ll.add(i);
    }
    for (Integer i = Integer.MAX_VALUE - 1000; i <= Integer.MAX_VALUE && i > 0; i++) {
      map.put(i, "");
      ll.add(i);
    }

    db.commit();

    db.clearCache();
    for (Integer i : ll) {
      assertTrue("failed for " + i, map.containsKey(i));
    }

    assertTrue(!map.containsKey(Integer.valueOf(Integer.MIN_VALUE + 1000)));
    assertTrue(!map.containsKey(Integer.valueOf(Integer.MIN_VALUE + 1001)));
    assertTrue(!map.containsKey(Integer.valueOf(-1001)));
    assertTrue(!map.containsKey(Integer.valueOf(-1002)));
    assertTrue(!map.containsKey(Integer.valueOf(1001)));
    assertTrue(!map.containsKey(Integer.valueOf(1002)));
    assertTrue(!map.containsKey(Integer.valueOf(Integer.MAX_VALUE - 1001)));
    assertTrue(!map.containsKey(Integer.valueOf(Integer.MAX_VALUE - 1002)));

    db.close();
  }

  public void testStrings() throws IOException {
    long init = Long.MAX_VALUE - size * 2;
    String file = newTestFile();
    DB db = new DBStore(file, false, false, false);
    SortedMap<String, String> map = db.createTreeMap("aa");
    for (long i = init; i < init + size / 10; i++) {
      map.put("aaaaa" + i, "");
    }
    db.commit();
    db.defrag(true);
    db.close();
    db = new DBStore(file, false, false, false);
    map = db.getTreeMap("aa");
    for (long i = init; i < init + size / 10; i++) {
      assertTrue(map.containsKey("aaaaa" + i));
    }

    long fileSize = new File(file + ".dbr.0").length() / 1024;
    System.out.println("file size with Strings: " + fileSize);
    assertTrue("file is too big, compression failed", fileSize < 120);
  }

}
