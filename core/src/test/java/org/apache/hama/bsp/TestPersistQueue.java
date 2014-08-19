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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.sync.SyncException;

public class TestPersistQueue extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestPartitioning.class);

  public void testDiskQueue() throws Exception {
    BSPJob bsp = getNewJobConf();
    bsp.set(MessageManager.RECEIVE_QUEUE_TYPE_CLASS,
        "org.apache.hama.bsp.message.queue.DiskQueue");

    assertTrue(bsp.waitForCompletion(true));
  }

  public void testMemoryQueue() throws Exception {
    BSPJob bsp = getNewJobConf();
    bsp.set(MessageManager.RECEIVE_QUEUE_TYPE_CLASS,
        "org.apache.hama.bsp.message.queue.MemoryQueue");

    assertTrue(bsp.waitForCompletion(true));
  }

  public void testSortedQueue() throws Exception {
    BSPJob bsp = getNewJobConf();
    bsp.set(MessageManager.RECEIVE_QUEUE_TYPE_CLASS,
        "org.apache.hama.bsp.message.queue.SortedMemoryQueue");

    assertTrue(bsp.waitForCompletion(true));
  }

  public void testSpillingQueue() throws Exception {
    BSPJob bsp = getNewJobConf();
    bsp.set(MessageManager.RECEIVE_QUEUE_TYPE_CLASS,
        "org.apache.hama.bsp.message.queue.SpillingQueue");

    assertTrue(bsp.waitForCompletion(true));
  }

  public BSPJob getNewJobConf() throws Exception {
    Configuration conf = new Configuration();
    BSPJob bsp = new BSPJob(new HamaConfiguration(conf));
    bsp.setJobName("Test persistent behaviour");
    bsp.setBspClass(persistentMsgBSP.class);
    bsp.setNumBspTask(2);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputFormat(NullOutputFormat.class);
    bsp.setMessageQueueBehaviour(MessageQueue.PERSISTENT_QUEUE);
    return bsp;
  }

  public static class persistentMsgBSP extends
      BSP<LongWritable, Text, NullWritable, NullWritable, IntWritable> {

    @Override
    public void bsp(
        BSPPeer<LongWritable, Text, NullWritable, NullWritable, IntWritable> peer)
        throws IOException, SyncException, InterruptedException {

      for (int i = 0; i < 10; i++) {
        peer.send(peer.getPeerName(0), new IntWritable(i));
        peer.send(peer.getPeerName(1), new IntWritable(i));
        peer.sync();

        if ((peer.getSuperstepCount() % 2) == 0) {
          peer.getCurrentMessage();
        }
      }

      int cnt = 0;
      while ((peer.getCurrentMessage()) != null) {
        cnt++;
      }

      assertTrue(cnt == 15);
    }
  }
}
