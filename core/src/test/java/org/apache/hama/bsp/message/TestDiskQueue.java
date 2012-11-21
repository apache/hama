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

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.junit.Test;

public class TestDiskQueue extends TestCase {

  static Configuration conf = new Configuration();
  static {
    conf.set(DiskQueue.DISK_QUEUE_PATH_KEY,
        TestAvroMessageManager.TMP_OUTPUT_PATH);
  }

  @Test
  public void testDiskQueue() throws Exception {
    DiskQueue<IntWritable> queue = getQueue();
    checkQueue(queue);
    queue.close();
  }

  @Test
  public void testMultipleIterations() throws Exception {
    DiskQueue<IntWritable> queue = getQueue();
    for (int superstep = 0; superstep < 15; superstep++) {
      checkQueue(queue);
    }
    queue.close();
  }

  public DiskQueue<IntWritable> getQueue() {
    TaskAttemptID id = new TaskAttemptID(new TaskID("123", 1, 2), 0);
    DiskQueue<IntWritable> queue = new DiskQueue<IntWritable>();
    // normally this is injected via reflection
    queue.setConf(conf);
    queue.init(conf, id);
    return queue;
  }

  public void checkQueue(DiskQueue<IntWritable> queue) {
    queue.prepareWrite();
    for (int i = 0; i < 10; i++) {
      queue.add(new IntWritable(i));
    }

    queue.addAll(Arrays.asList(new IntWritable(11), new IntWritable(12),
        new IntWritable(13)));
    assertEquals(13, queue.size());
    queue.prepareRead();

    for (int i = 0; i < 9; i++) {
      assertEquals(i, queue.poll().get());
    }

    queue.clear();

    assertEquals(0, queue.size());
  }

}
