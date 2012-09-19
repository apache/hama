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

public class DBCacheMRUTest extends TestCaseWithTestFile {

  public void testPurgeEntryClearsCache() throws IOException {
    DBCacheMRU d = (DBCacheMRU) newDBCache();

    for (long i = 0; i < 1e3; i++)
      d.addEntry(newEntry(i));

    for (long i = 0; i < 1e3; i++)
      d.purgeEntry();

    assertEquals(d._hash.size(), 0);
  }

  DBCacheMRU.CacheEntry newEntry(long i) {
    return new DBCacheMRU.CacheEntry(i, i);
  }

  public void testCacheMaxSize() throws IOException {

    DBCacheMRU d = (DBCacheMRU) DBMaker.openFile(newTestFile())
        .setMRUCacheSize(100).make();

    ArrayList<Long> recids = new ArrayList<Long>();
    for (int i = 0; i < 1e5; i++) {
      recids.add(d.insert("aa" + i));
    }
    d.commit();
    for (int i = 0; i < 1e5; i++) {
      d.fetch(recids.get(i));
    }

    assert (d._hash.size() <= 100);

  }
}
