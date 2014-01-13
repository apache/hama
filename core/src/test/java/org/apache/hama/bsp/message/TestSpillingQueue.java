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

import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.bsp.message.queue.SpillingQueue;

public class TestSpillingQueue extends TestCase {

  /**
   * Test the spilling queue where the message class is specified.
   * 
   * @throws Exception
   */
  public void testTextSpillingQueue() throws Exception {

    String msg = "Testing the spillage of spilling buffer";
    Text text = new Text(msg);
    TaskAttemptID id = new TaskAttemptID(new TaskID("123", 1, 2), 0);
    SpillingQueue<Text> queue = new SpillingQueue<Text>();
    Configuration conf = new HamaConfiguration();

    String fileName = System.getProperty("java.io.tmpdir") + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);
    File file = new File(fileName);
    conf.set(SpillingQueue.SPILLBUFFER_FILENAME, fileName);
    conf.setClass(SpillingQueue.SPILLBUFFER_MSGCLASS, Text.class,
        Writable.class);
    queue.init(conf, id);
    queue.prepareWrite();
    for (int i = 0; i < 1000; ++i) {
      queue.add(text);
    }
    assertEquals(queue.size(), 1000);
    queue.prepareRead();
    Text t;
    while ((t = queue.poll()) != null) {
      assertTrue(msg.equals(t.toString()));
    }
    assertEquals(queue.size(), 0);

    assertTrue(queue.poll() == null);

    assertTrue(file.exists());
    queue.close();
    assertFalse(file.exists());
  }

  /**
   * Test the spilling queue where the message class is not specified and the
   * queue uses ObjectWritable to store messages.
   * 
   * @throws Exception
   */
  public void testObjectWritableSpillingQueue() throws Exception {

    String msg = "Testing the spillage of spilling buffer";
    Text text = new Text(msg);
    TaskAttemptID id = new TaskAttemptID(new TaskID("123", 1, 2), 0);
    SpillingQueue<Text> queue = new SpillingQueue<Text>();
    Configuration conf = new HamaConfiguration();

    String fileName = System.getProperty("java.io.tmpdir") + File.separatorChar
        + new BigInteger(128, new SecureRandom()).toString(32);
    File file = new File(fileName);
    conf.set(SpillingQueue.SPILLBUFFER_FILENAME, fileName);
    queue.init(conf, id);
    queue.prepareWrite();
    for (int i = 0; i < 1000; ++i) {
      queue.add(text);
    }
    queue.prepareRead();
    for (Text t : queue) {
      assertTrue(msg.equals(t.toString()));
      text.clear();
    }

    assertTrue(queue.poll() == null);

    assertTrue(file.exists());
    queue.close();
    assertFalse(file.exists());
  }

}
