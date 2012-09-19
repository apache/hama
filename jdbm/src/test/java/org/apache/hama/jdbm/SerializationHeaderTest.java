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

import java.lang.reflect.Field;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.TestCase;

public class SerializationHeaderTest extends TestCase {

  public void testUnique() throws IllegalAccessException {
    Class c = SerializationHeader.class;
    Set<Integer> s = new TreeSet<Integer>();
    for (Field f : c.getDeclaredFields()) {
      f.setAccessible(true);
      int value = f.getInt(null);

      assertTrue("Value already used: " + value, !s.contains(value));
      s.add(value);
    }
    assertTrue(!s.isEmpty());
  }
}
