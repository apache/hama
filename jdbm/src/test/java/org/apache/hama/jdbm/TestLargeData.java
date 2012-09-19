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
import java.util.Arrays;

public class TestLargeData extends TestCaseWithTestFile {

  public void testLargeData() throws IOException {

    DBAbstract db = new DBStore(newTestFile(), false, false, false);

    byte[] data = UtilTT.makeRecord(1000000, (byte) 12);
    final long id = db.insert(data);
    data = (byte[]) db.fetch(id);
    UtilTT.checkRecord(data, 1000000, (byte) 12);
    db.commit();

    data = UtilTT.makeRecord(2000000, (byte) 13);
    db.update(id, data);
    db.commit();
    data = (byte[]) db.fetch(id);
    UtilTT.checkRecord(data, 2000000, (byte) 13);
    db.commit();

    data = UtilTT.makeRecord(1500000, (byte) 14);
    db.update(id, data);
    data = (byte[]) db.fetch(id);
    UtilTT.checkRecord(data, 1500000, (byte) 14);
    db.commit();

    data = UtilTT.makeRecord(2500000, (byte) 15);
    db.update(id, data);
    db.rollback();
    data = (byte[]) db.fetch(id);
    UtilTT.checkRecord(data, 1500000, (byte) 14);
    db.commit();

    data = UtilTT.makeRecord(1, (byte) 20);
    db.update(id, data);
    data = (byte[]) db.fetch(id);
    UtilTT.checkRecord(data, 1, (byte) 20);
    db.commit();
  }

  public void testAllSizes() throws IOException {
    // use in memory store to make it faster
    DBStore db = (DBStore) DBMaker.openFile(newTestFile()).disableCache()
        .disableTransactions().make();
    for (int i = 1; i < RecordHeader.MAX_RECORD_SIZE - 100; i += 111111) {
      // System.out.println(i);
      byte[] rec = UtilTT.makeRecord(i, (byte) 11);
      long recid = db.insert(rec);
      byte[] rec2 = db.fetch(recid);
      assertTrue("error at size: " + i, Arrays.equals(rec, rec2));
      db.delete(recid);
    }
  }

}
