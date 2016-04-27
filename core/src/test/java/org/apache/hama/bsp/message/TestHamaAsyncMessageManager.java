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

import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.Counters;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.queue.MemoryQueue;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.util.BSPNetUtils;

public class TestHamaAsyncMessageManager extends TestCase {

  public static final String TMP_OUTPUT_PATH = "/tmp/messageQueue";
  // increment is here to solve race conditions in parallel execution to choose
  // other ports
  public static volatile int increment = 1;

  public void testMemoryMessaging() throws Exception {
    if(!SystemUtils.IS_OS_LINUX) {
      System.out.println("Skipping testcase because Async is only supported for LINUX!");
      return;
    }
      
    HamaConfiguration conf = new HamaConfiguration();
    conf.setClass(MessageManager.RECEIVE_QUEUE_TYPE_CLASS, MemoryQueue.class,
        MessageQueue.class);
    messagingInternal(conf);
  }

  private static void messagingInternal(HamaConfiguration conf)
      throws Exception {
    conf.set(MessageManagerFactory.MESSAGE_MANAGER_CLASS,
        "org.apache.hama.bsp.message.HamaAsyncMessageManagerImpl");
    MessageManager<IntWritable> messageManager = MessageManagerFactory
        .getMessageManager(conf);

    assertTrue(messageManager instanceof HamaAsyncMessageManagerImpl);

    InetSocketAddress peer = new InetSocketAddress(
        BSPNetUtils.getCanonicalHostname(), BSPNetUtils.getFreePort()
            + (increment++));
    conf.set(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST);
    conf.setInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);

    BSPPeer<?, ?, ?, ?, IntWritable> dummyPeer = new BSPPeerImpl<NullWritable, NullWritable, NullWritable, NullWritable, IntWritable>(
        conf, FileSystem.get(conf), new Counters());
    TaskAttemptID id = new TaskAttemptID("1", 1, 1, 1);
    messageManager.init(id, dummyPeer, conf, peer);
    peer = messageManager.getListenerAddress();
    String peerName = peer.getHostName() + ":" + peer.getPort();
    System.out.println("Peer is " + peerName);
    messageManager.send(peerName, new IntWritable(1337));

    Iterator<Entry<InetSocketAddress, BSPMessageBundle<IntWritable>>> messageIterator = messageManager
        .getOutgoingBundles();

    Entry<InetSocketAddress, BSPMessageBundle<IntWritable>> entry = messageIterator
        .next();

    assertEquals(entry.getKey(), peer);

    assertTrue(entry.getValue().size() == 1);

    BSPMessageBundle<IntWritable> bundle = new BSPMessageBundle<IntWritable>();
    Iterator<IntWritable> it = entry.getValue().iterator();
    while (it.hasNext()) {
      bundle.addMessage(it.next());
    }

    messageManager.transfer(peer, bundle);

    messageManager.clearOutgoingMessages();

    assertTrue(messageManager.getNumCurrentMessages() == 1);
    IntWritable currentMessage = messageManager.getCurrentMessage();

    assertEquals(currentMessage.get(), 1337);
    messageManager.close();
  }
}
