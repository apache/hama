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

import java.io.EOFException;
import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;

import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.message.io.SpilledDataInputBuffer;
import org.apache.hama.bsp.message.io.SpillingDataOutputBuffer;
import org.apache.hama.bsp.message.io.WriteSpilledDataProcessor;

import junit.framework.TestCase;

public class TestMessageIO extends TestCase {

  public void testNonSpillBuffer() throws Exception {

    SpillingDataOutputBuffer outputBuffer = new SpillingDataOutputBuffer();
    Text text = new Text("Testing the spillage of spilling buffer");

    for (int i = 0; i < 100; ++i) {
      text.write(outputBuffer);
    }

    assertTrue(outputBuffer != null);
    assertTrue(outputBuffer.size() == 4000);
    assertFalse(outputBuffer.hasSpilled());

    outputBuffer.close();

  }

  public void testSpillBuffer() throws Exception {

    String fileName = System.getProperty("java.io.tmpdir") + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);
    SpillingDataOutputBuffer outputBuffer = new SpillingDataOutputBuffer(2,
        1024, 1024, true, new WriteSpilledDataProcessor(fileName));
    Text text = new Text("Testing the spillage of spilling buffer");

    for (int i = 0; i < 100; ++i) {
      text.write(outputBuffer);
    }

    assertTrue(outputBuffer != null);
    assertTrue(outputBuffer.size() == 4000);
    assertTrue(outputBuffer.hasSpilled());
    File f = new File(fileName);
    assertTrue(f.exists());
    assertTrue(f.delete());
    outputBuffer.close();

  }

  public void testSpillInputStream() throws Exception {
    String fileName = System.getProperty("java.io.tmpdir") + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);
    SpillingDataOutputBuffer outputBuffer = new SpillingDataOutputBuffer(2,
        1024, 1024, true, new WriteSpilledDataProcessor(fileName));
    Text text = new Text("Testing the spillage of spilling buffer");

    for (int i = 0; i < 100; ++i) {
      text.write(outputBuffer);
    }

    assertTrue(outputBuffer != null);
    assertTrue(outputBuffer.size() == 4000);
    assertTrue(outputBuffer.hasSpilled());
    File f = new File(fileName);
    assertTrue(f.exists());
    outputBuffer.close();
    assertTrue(f.length() == 4000L);

    SpilledDataInputBuffer inputBuffer = outputBuffer
        .getInputStreamToRead(fileName);

    for (int i = 0; i < 100; ++i) {
      text.readFields(inputBuffer);
      assertTrue("Testing the spillage of spilling buffer".equals(text
          .toString()));
      text.clear();
    }

    try {
      text.readFields(inputBuffer);
      assertTrue(false);
    } catch (EOFException eof) {
      assertTrue(true);
    }

    inputBuffer.close();
    inputBuffer.completeReading(false);
    assertTrue(f.exists());
    inputBuffer.completeReading(true);
    assertFalse(f.exists());

  }

}
