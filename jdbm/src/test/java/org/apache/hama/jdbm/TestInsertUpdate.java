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

import org.junit.Test;

public class TestInsertUpdate extends TestCaseWithTestFile {

  /**
   * Test that the object is not modified by serialization.
   * 
   * @throws IOException
   */
  @Test
  public void testInsertUpdateWithCustomSerializer() throws IOException {
    DB db = newDBCache();
    Serializer<Long> serializer = new HTreeBucketTest.LongSerializer();

    Map<Long, Long> map = db.createHashMap("custom", serializer, serializer);

    map.put(new Long(1), new Long(1));
    map.put(new Long(2), new Long(2));
    db.commit();
    map.put(new Long(2), new Long(3));
    db.commit();
    db.close();
  }

}
