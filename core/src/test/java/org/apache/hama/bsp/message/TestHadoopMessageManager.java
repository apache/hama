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
import java.util.LinkedList;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.util.BSPNetUtils;

public class TestHadoopMessageManager extends TestCase {

  public void testMessaging() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MessageManagerFactory.MESSAGE_MANAGER_CLASS,
        "org.apache.hama.bsp.message.HadoopMessageManagerImpl");
    MessageManager<IntWritable> messageManager = MessageManagerFactory
        .getMessageManager(conf);

    assertTrue(messageManager instanceof HadoopMessageManagerImpl);

    InetSocketAddress peer = new InetSocketAddress(
        BSPNetUtils.getCanonicalHostname(), BSPNetUtils.getFreePort());
    messageManager.init(conf, peer);
    String peerName = peer.getHostName() + ":" + peer.getPort();

    messageManager.send(peerName, new IntWritable(1337));

    Iterator<Entry<InetSocketAddress, LinkedList<IntWritable>>> messageIterator = messageManager
        .getMessageIterator();

    Entry<InetSocketAddress, LinkedList<IntWritable>> entry = messageIterator
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
  }
}
