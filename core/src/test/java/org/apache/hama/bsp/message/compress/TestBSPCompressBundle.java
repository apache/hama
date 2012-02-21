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
package org.apache.hama.bsp.message.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

public class TestBSPCompressBundle extends TestCase {

  public void testBundle() throws Exception {
    BSPCompressedBundle bundle = new BSPCompressedBundle(
        "Hello World!".getBytes());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    bundle.write(dos);
    byte[] byteArray = bos.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(byteArray);
    DataInputStream dis = new DataInputStream(in);
    BSPCompressedBundle b = new BSPCompressedBundle();
    b.readFields(dis);

    String string = new String(b.getData());
    assertEquals("Hello World!", string);
  }

}
