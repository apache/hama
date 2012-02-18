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

public class TestBSPMessageBundle extends TestCase {

  public void testEmpty() throws IOException {
    BSPMessageBundle bundle = new BSPMessageBundle();
    // Serialize it.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bundle.write(new DataOutputStream(baos));
    baos.close();
    // Deserialize it.
    BSPMessageBundle readBundle = new BSPMessageBundle();
    readBundle.readFields(new DataInputStream(new ByteArrayInputStream(baos
        .toByteArray())));
    assertEquals(0, readBundle.getMessages().size());
  }

  public void testSerializationDeserialization() throws IOException {
    BSPMessageBundle bundle = new BSPMessageBundle();
    ByteMessage[] testMessages = new ByteMessage[16];
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
      testMessages[i] = new ByteMessage(tag, data);
      bundle.addMessage(testMessages[i]);
    }
    // Serialize it.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bundle.write(new DataOutputStream(baos));
    baos.close();
    // Deserialize it.
    BSPMessageBundle readBundle = new BSPMessageBundle();
    readBundle.readFields(new DataInputStream(new ByteArrayInputStream(baos
        .toByteArray())));
    // Check contents.
    int messageNumber = 0;
    for (BSPMessage message : readBundle.getMessages()) {
      ByteMessage byteMessage = (ByteMessage) message;
      assertTrue(Arrays.equals(testMessages[messageNumber].getTag(),
          byteMessage.getTag()));
      assertTrue(Arrays.equals(testMessages[messageNumber].getData(),
          byteMessage.getData()));
      ++messageNumber;
    }
    assertEquals(testMessages.length, messageNumber);
  }
}
