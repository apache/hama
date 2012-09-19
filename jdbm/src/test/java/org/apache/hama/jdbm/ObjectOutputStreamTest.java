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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hama.jdbm.SerialClassInfoTest.Bean1;

public class ObjectOutputStreamTest extends TestCase {

  @SuppressWarnings("unchecked")
  <E> E neser(E e) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream i = new ByteArrayOutputStream();
    new AdvancedObjectOutputStream(i).writeObject(e);
    return (E) new AdvancedObjectInputStream(new ByteArrayInputStream(
        i.toByteArray())).readObject();
  }

  public void testSimple() throws ClassNotFoundException, IOException {

    Bean1 b = new Bean1("qwe", "rty");
    Bean1 b2 = neser(b);

    assertEquals(b, b2);

  }
}
