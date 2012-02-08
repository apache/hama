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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.util.BSPNetUtils;

public class AvroMessageManagerImpl implements MessageManager, Sender {

  private static final Log LOG = LogFactory
      .getLog(AvroMessageManagerImpl.class);

  private NettyServer server = null;

  private final HashMap<InetSocketAddress, Sender> peers = new HashMap<InetSocketAddress, Sender>();
  private final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();

  private final HashMap<InetSocketAddress, LinkedList<BSPMessage>> outgoingQueues = new HashMap<InetSocketAddress, LinkedList<BSPMessage>>();
  private Deque<BSPMessage> localQueue = new LinkedList<BSPMessage>();
  // this must be a synchronized implementation: this is accessed per RPC
  private final ConcurrentLinkedQueue<BSPMessage> localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();

  @Override
  public void init(Configuration conf, InetSocketAddress addr) {
    server = new NettyServer(new SpecificResponder(Sender.class, this), addr);
  }

  @Override
  public void close() {
    server.close();
  }

  @Override
  public void clearOutgoingQueues() {
    this.outgoingQueues.clear();
    localQueue.addAll(localQueueForNextIteration);
    localQueueForNextIteration.clear();
  }

  public void put(BSPMessageBundle messages) {
    for (BSPMessage message : messages.getMessages()) {
      this.localQueueForNextIteration.add(message);
    }
  }

  @Override
  public int getNumCurrentMessages() {
    return localQueue.size();
  }

  @Override
  public void transfer(InetSocketAddress addr, BSPMessageBundle bundle)
      throws IOException {
    AvroBSPMessageBundle msg = new AvroBSPMessageBundle();
    msg.setData(serializeMessage(bundle));
    Sender sender = peers.get(addr);

    if (sender == null) {
      NettyTransceiver client = new NettyTransceiver(addr);
      sender = (Sender) SpecificRequestor.getClient(Sender.class, client);
      peers.put(addr, sender);
    }
    
    sender.transfer(msg);
  }

  @Override
  public Void transfer(AvroBSPMessageBundle messagebundle)
      throws AvroRemoteException {
    try {
      BSPMessageBundle deserializeMessage = deserializeMessage(messagebundle
          .getData());
      this.put(deserializeMessage);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public BSPMessage getCurrentMessage() throws IOException {
    return localQueue.poll();
  }

  @Override
  public void send(String peerName, BSPMessage msg) throws IOException {
    LOG.debug("Send message (" + msg.toString() + ") to " + peerName);
    InetSocketAddress targetPeerAddress = null;
    // Get socket for target peer.
    if (peerSocketCache.containsKey(peerName)) {
      targetPeerAddress = peerSocketCache.get(peerName);
    } else {
      targetPeerAddress = BSPNetUtils.getAddress(peerName);
      peerSocketCache.put(peerName, targetPeerAddress);
    }
    LinkedList<BSPMessage> queue = outgoingQueues.get(targetPeerAddress);
    if (queue == null) {
      queue = new LinkedList<BSPMessage>();
    }
    queue.add(msg);
    outgoingQueues.put(targetPeerAddress, queue);
  }

  private static final BSPMessageBundle deserializeMessage(ByteBuffer buffer)
      throws IOException {
    BSPMessageBundle msg = new BSPMessageBundle();

    ByteArrayInputStream inArray = new ByteArrayInputStream(buffer.array());
    DataInputStream in = new DataInputStream(inArray);
    msg.readFields(in);

    return msg;
  }

  private static final ByteBuffer serializeMessage(BSPMessageBundle msg)
      throws IOException {
    ByteArrayOutputStream outArray = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(outArray);
    msg.write(out);
    out.close();
    System.out.println("serialized " + outArray.size() + " bytes");
    return ByteBuffer.wrap(outArray.toByteArray());
  }

  @Override
  public Iterator<Entry<InetSocketAddress, LinkedList<BSPMessage>>> getMessageIterator() {
    return this.outgoingQueues.entrySet().iterator();
  }
}
