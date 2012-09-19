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

import junit.framework.TestCase;

public class DataInputOutputTest extends TestCase {

  final DataInputOutput d = new DataInputOutput();

  public void testInt() throws IOException {
    int i = 123129049;
    d.writeInt(i);
    d.reset();
    assertEquals(i, d.readInt());
  }

  public void testLong() throws IOException {
    long i = 1231290495545446485L;
    d.writeLong(i);
    d.reset();
    assertEquals(i, d.readLong());
  }

  public void testBooelean() throws IOException {
    d.writeBoolean(true);
    d.reset();
    assertEquals(true, d.readBoolean());
    d.reset();
    d.writeBoolean(false);
    d.reset();
    assertEquals(false, d.readBoolean());

  }

  public void testByte() throws IOException {

    for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
      d.writeByte(i);
      d.reset();
      assertEquals(i, d.readByte());
      d.reset();
    }
  }

  public void testUnsignedByte() throws IOException {

    for (int i = 0; i <= 255; i++) {
      d.write(i);
      d.reset();
      assertEquals(i, d.readUnsignedByte());
      d.reset();
    }
  }

  public void testLongPacker() throws IOException {

    for (int i = 0; i < 1e7; i++) {
      LongPacker.packInt(d, i);
      d.reset();
      assertEquals(i, LongPacker.unpackInt(d));
      d.reset();
    }
  }

}
