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
package org.apache.hama.bsp.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.message.bundle.POJOMessageBundle;
import org.apache.hama.bsp.message.bundle.WritableMessageBundle;

public class TestMessageBundle extends TestCase {

  public void testPOJOWritableMessageBundle() {

    POJOMessageBundle<IntWritable> messageBundle = new POJOMessageBundle<IntWritable>();
    for (int i = 0; i < 100; ++i) {
      messageBundle.addMessage(new IntWritable(i));
    }
    assertEquals(100, messageBundle.getSize());
    assertEquals(100, messageBundle.getNumElements());

    int i = 0;
    for (IntWritable writable : messageBundle) {
      assertEquals(i++, writable.get());
    }

  }

  public void testDifferentWritableMessageBundle() {
    WritableMessageBundle<Writable> messageBundle = new WritableMessageBundle<Writable>();
    int numElements = 5;

    HashSet<Writable> set = new HashSet<Writable>();

    for (int i = 0; i < numElements; ++i) {
      Writable w = new IntWritable(i);
      set.add(w);
      messageBundle.addMessage(w);
    }
    String msg;
    for (int i = 0; i < numElements; ++i) {
      msg = "" + i;
      Writable w = new Text(msg);
      set.add(w);
      messageBundle.addMessage(w);
    }

    assertEquals(2 * numElements, messageBundle.getSize());
    assertEquals(2 * numElements, messageBundle.getNumElements());

    for (Writable writable : messageBundle) {
      set.remove(writable);
    }
    assertTrue(set.isEmpty());

  }

  public void testReadWriteWritableMessageBundle() throws IOException {
    WritableMessageBundle<Writable> messageBundle = new WritableMessageBundle<Writable>();
    int numElements = 5;

    HashSet<Writable> set = new HashSet<Writable>();

    for (int i = 0; i < numElements; ++i) {
      Writable w = new IntWritable(i);
      set.add(w);
      messageBundle.addMessage(w);
    }
    String msg;
    for (int i = 0; i < numElements; ++i) {
      msg = "" + i;
      Writable w = new Text(msg);
      set.add(w);
      messageBundle.addMessage(w);
    }

    assertEquals(2 * numElements, messageBundle.getSize());
    assertEquals(2 * numElements, messageBundle.getNumElements());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);
    DataOutput output = new DataOutputStream(outputStream);
    messageBundle.write(output);

    ByteArrayInputStream inStream = new ByteArrayInputStream(
        outputStream.toByteArray());
    DataInput in = new DataInputStream(inStream);
    WritableMessageBundle<Writable> newBundle = new WritableMessageBundle<Writable>();
    newBundle.readFields(in);

    for (Writable writable : newBundle) {
      set.remove(writable);
    }
    assertTrue(set.isEmpty());

  }

}
