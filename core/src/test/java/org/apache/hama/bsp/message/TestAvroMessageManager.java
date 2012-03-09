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
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.Counters;
import org.apache.hama.bsp.message.compress.BSPMessageCompressorFactory;
import org.apache.hama.bsp.message.compress.SnappyCompressor;
import org.apache.hama.bsp.messages.BooleanMessage;
import org.apache.hama.bsp.messages.DoubleMessage;
import org.apache.hama.bsp.messages.IntegerMessage;
import org.apache.hama.util.BSPNetUtils;

public class TestAvroMessageManager extends TestCase {

  private static final int DOUBLE_MSG_COUNT = 4000;
  private static final int BOOL_MSG_COUNT = 1000;
  private static final int INT_MSG_COUNT = 5000;

  private static final int SUM = DOUBLE_MSG_COUNT + BOOL_MSG_COUNT
      + INT_MSG_COUNT;

  public void testAvroMessenger() throws Exception {
    BSPMessageBundle<Writable> randomBundle = getRandomBundle();
    Configuration conf = new Configuration();
    MessageManager<Writable> messageManager = MessageManagerFactory
        .getMessageManager(conf);
    conf.set(BSPMessageCompressorFactory.COMPRESSION_CODEC_CLASS,
        SnappyCompressor.class.getName());

    assertTrue(messageManager instanceof AvroMessageManagerImpl);

    InetSocketAddress peer = new InetSocketAddress(
        BSPNetUtils.getCanonicalHostname(), BSPNetUtils.getFreePort());

    BSPPeer<?, ?, ?, ?, Writable> dummyPeer = new BSPPeerImpl<NullWritable, NullWritable, NullWritable, NullWritable, Writable>(
        conf, FileSystem.get(conf), new Counters());

    messageManager.init(dummyPeer, conf, peer);

    messageManager.transfer(peer, randomBundle);

    messageManager.clearOutgoingQueues();

    assertEquals(SUM, messageManager.getNumCurrentMessages());

    int numIntMsgs = 0, numBoolMsgs = 0, numDoubleMsgs = 0;

    Writable msg = null;
    while ((msg = messageManager.getCurrentMessage()) != null) {
      if (msg instanceof IntegerMessage) {
        numIntMsgs++;
      } else if (msg instanceof BooleanMessage) {
        numBoolMsgs++;
      } else if (msg instanceof DoubleMessage) {
        numDoubleMsgs++;
      }
    }

    assertEquals(INT_MSG_COUNT, numIntMsgs);
    assertEquals(BOOL_MSG_COUNT, numBoolMsgs);
    assertEquals(DOUBLE_MSG_COUNT, numDoubleMsgs);

  }

  public final BSPMessageBundle<Writable> getRandomBundle() {
    BSPMessageBundle<Writable> bundle = new BSPMessageBundle<Writable>();

    for (int i = 0; i < INT_MSG_COUNT; i++) {
      bundle.addMessage(new IntegerMessage("test", i));
    }

    for (int i = 0; i < BOOL_MSG_COUNT; i++) {
      bundle.addMessage(new BooleanMessage("test123", i % 2 == 0));
    }

    Random r = new Random();
    for (int i = 0; i < DOUBLE_MSG_COUNT; i++) {
      bundle.addMessage(new DoubleMessage("123123asd", r.nextDouble()));
    }

    return bundle;
  }

}
