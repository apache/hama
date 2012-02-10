/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.io.BytesWritable;;

public class TestBSPMessageBundle extends TestCase {

  public void testEmpty() throws IOException {
    BSPMessageBundle<BytesWritable> bundle = new BSPMessageBundle<BytesWritable>();
    // Serialize it.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bundle.write(new DataOutputStream(baos));
    baos.close();
    // Deserialize it.
    BSPMessageBundle<BytesWritable> readBundle = new BSPMessageBundle<BytesWritable>();
    readBundle.readFields(new DataInputStream(new ByteArrayInputStream(baos
        .toByteArray())));
    assertEquals(0, readBundle.getMessages().size());
  }

  public void testSerializationDeserialization() throws IOException {
    BSPMessageBundle<BytesWritable> bundle = new BSPMessageBundle<BytesWritable>();
    BytesWritable[] testMessages = new BytesWritable[16];
    for (int i = 0; i < testMessages.length; ++i) {
      // Create a one byte tag containing the number of the message.
      byte[] tag = new byte[1];
      tag[0] = (byte) i;
      // Create a four bytes data part containing serialized number of the
      // message.
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(i);
      baos.close();
      byte[] data = baos.toByteArray();
      BytesWritable msg = new BytesWritable();
      msg.set(data, 0, data.length);
      testMessages[i] = msg;
      bundle.addMessage(testMessages[i]);
    }
    // Serialize it.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bundle.write(new DataOutputStream(baos));
    baos.close();
    // Deserialize it.
    BSPMessageBundle<BytesWritable> readBundle = new BSPMessageBundle<BytesWritable>();
    readBundle.readFields(new DataInputStream(new ByteArrayInputStream(baos
        .toByteArray())));
    // Check contents.
    int messageNumber = 0;
    for (BytesWritable message : readBundle.getMessages()) {
      BytesWritable byteMessage = message;

      assertTrue(Arrays.equals(testMessages[messageNumber].getBytes(),
          byteMessage.getBytes()));
      ++messageNumber;
    }
    assertEquals(testMessages.length, messageNumber);
  }
}
