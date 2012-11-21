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

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.Counters;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.apache.hama.bsp.message.queue.MemoryQueue;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.util.BSPNetUtils;

public class TestHadoopMessageManager extends TestCase {

  public static final String TMP_OUTPUT_PATH = "/tmp/messageQueue";
  // increment is here to solve race conditions in parallel execution to choose
  // other ports.
  public static volatile int increment = 1;

  public void testMemoryMessaging() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MessageManager.QUEUE_TYPE_CLASS,
        MemoryQueue.class.getCanonicalName());
    conf.set(DiskQueue.DISK_QUEUE_PATH_KEY, TMP_OUTPUT_PATH);
    messagingInternal(conf);
  }

  public void testDiskMessaging() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DiskQueue.DISK_QUEUE_PATH_KEY, TMP_OUTPUT_PATH);
    messagingInternal(conf);
  }

  private static void messagingInternal(Configuration conf) throws Exception {
    conf.set(MessageManagerFactory.MESSAGE_MANAGER_CLASS,
        "org.apache.hama.bsp.message.HadoopMessageManagerImpl");
    MessageManager<IntWritable> messageManager = MessageManagerFactory
        .getMessageManager(conf);

    assertTrue(messageManager instanceof HadoopMessageManagerImpl);

    InetSocketAddress peer = new InetSocketAddress(
        BSPNetUtils.getCanonicalHostname(), BSPNetUtils.getFreePort()
            + (increment++));
    BSPPeer<?, ?, ?, ?, IntWritable> dummyPeer = new BSPPeerImpl<NullWritable, NullWritable, NullWritable, NullWritable, IntWritable>(
        conf, FileSystem.get(conf), new Counters());
    TaskAttemptID id = new TaskAttemptID("1", 1, 1, 1);
    messageManager.init(id, dummyPeer, conf, peer);
    String peerName = peer.getHostName() + ":" + peer.getPort();

    messageManager.send(peerName, new IntWritable(1337));

    Iterator<Entry<InetSocketAddress, MessageQueue<IntWritable>>> messageIterator = messageManager
        .getMessageIterator();

    Entry<InetSocketAddress, MessageQueue<IntWritable>> entry = messageIterator
        .next();

    assertEquals(entry.getKey(), peer);

    assertTrue(entry.getValue().size() == 1);

    BSPMessageBundle<IntWritable> bundle = new BSPMessageBundle<IntWritable>();
    for (IntWritable msg : entry.getValue()) {
      bundle.addMessage(msg);
    }

    messageManager.transfer(peer, bundle);

    messageManager.clearOutgoingQueues();

    assertTrue(messageManager.getNumCurrentMessages() == 1);
    IntWritable currentMessage = messageManager.getCurrentMessage();

    assertEquals(currentMessage.get(), 1337);
    messageManager.close();
  }
}
