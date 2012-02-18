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
package org.apache.hama.bsp;

import junit.framework.TestCase;

import org.apache.hama.util.Bytes;

public class TestMessages extends TestCase {

  public void testByteMessage() {
    int dataSize = (int) (Runtime.getRuntime().maxMemory() * 0.60);
    ByteMessage msg = new ByteMessage(Bytes.toBytes("tag"), new byte[dataSize]);
    assertEquals(msg.getData().length, dataSize);
    msg = null;
    
    byte[] dummyData = new byte[1024];
    ByteMessage msg2 = new ByteMessage(Bytes.tail(dummyData, 128), dummyData);
    assertEquals(
        Bytes.compareTo(msg2.getTag(), 0, 128, msg2.getData(),
            msg2.getData().length - 128, 128), 0);
  }
}
