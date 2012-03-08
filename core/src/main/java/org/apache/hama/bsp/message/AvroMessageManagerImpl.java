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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.message.compress.BSPCompressedBundle;
import org.apache.hama.util.BSPNetUtils;

public final class AvroMessageManagerImpl<M extends Writable> extends
    CompressableMessageManager<M> implements Sender<M> {

  private NettyServer server = null;

  private final HashMap<InetSocketAddress, Sender<M>> peers = new HashMap<InetSocketAddress, Sender<M>>();
  private final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();

  private final HashMap<InetSocketAddress, LinkedList<M>> outgoingQueues = new HashMap<InetSocketAddress, LinkedList<M>>();
  private Deque<M> localQueue = new LinkedList<M>();
  // this must be a synchronized implementation: this is accessed per RPC
  private final ConcurrentLinkedQueue<M> localQueueForNextIteration = new ConcurrentLinkedQueue<M>();

  private BSPPeer<?, ?, ?, ?, M> peer;

  @Override
  public void init(BSPPeer<?, ?, ?, ?, M> peer, Configuration conf,
      InetSocketAddress addr) {
    this.peer = peer;
    super.initCompression(conf);
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

  public void put(BSPMessageBundle<M> messages) {
    peer.incrementCounter(
        BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED, messages.getMessages()
            .size());
    Iterator<M> iterator = messages.getMessages().iterator();
    while (iterator.hasNext()) {
      this.localQueueForNextIteration.add(iterator.next());
      iterator.remove();
    }
  }

  @Override
  public int getNumCurrentMessages() {
    return localQueue.size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
      throws IOException {
    AvroBSPMessageBundle<M> msg = new AvroBSPMessageBundle<M>();
    msg.setData(serializeMessage(bundle));
    Sender<M> sender = peers.get(addr);

    if (sender == null) {
      NettyTransceiver client = new NettyTransceiver(addr);
      sender = (Sender<M>) SpecificRequestor.getClient(Sender.class, client);
      peers.put(addr, sender);
    }

    sender.transfer(msg);
  }

  @Override
  public Void transfer(AvroBSPMessageBundle<M> messagebundle)
      throws AvroRemoteException {
    try {
      BSPMessageBundle<M> deserializeMessage = deserializeMessage(messagebundle
          .getData());
      this.put(deserializeMessage);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public M getCurrentMessage() throws IOException {
    return localQueue.poll();
  }

  @Override
  public void send(String peerName, M msg) throws IOException {
    InetSocketAddress targetPeerAddress = null;
    // Get socket for target peer.
    if (peerSocketCache.containsKey(peerName)) {
      targetPeerAddress = peerSocketCache.get(peerName);
    } else {
      targetPeerAddress = BSPNetUtils.getAddress(peerName);
      peerSocketCache.put(peerName, targetPeerAddress);
    }
    LinkedList<M> queue = outgoingQueues.get(targetPeerAddress);
    if (queue == null) {
      queue = new LinkedList<M>();
    }
    queue.add(msg);
    outgoingQueues.put(targetPeerAddress, queue);
  }

  private final BSPMessageBundle<M> deserializeMessage(ByteBuffer buffer)
      throws IOException {
    BSPMessageBundle<M> msg = new BSPMessageBundle<M>();
    byte[] byteArray = buffer.array();
    if (compressor == null) {
      peer.incrementCounter(
          BSPPeerImpl.PeerCounter.MESSAGE_BYTES_RECEIVED, byteArray.length);
      ByteArrayInputStream inArray = new ByteArrayInputStream(byteArray);
      DataInputStream in = new DataInputStream(inArray);
      msg.readFields(in);
    } else {
      peer.incrementCounter(
          BSPPeerImpl.PeerCounter.COMPRESSED_BYTES_RECEIVED, byteArray.length);
      msg = compressor.decompressBundle(new BSPCompressedBundle(byteArray));
    }

    return msg;
  }

  private final ByteBuffer serializeMessage(BSPMessageBundle<M> msg)
      throws IOException {
    if (compressor == null) {
      ByteArrayOutputStream outArray = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(outArray);
      msg.write(out);
      out.close();
      byte[] byteArray = outArray.toByteArray();
      peer.incrementCounter(
          BSPPeerImpl.PeerCounter.MESSAGE_BYTES_TRANSFERED, byteArray.length);
      return ByteBuffer.wrap(byteArray);
    } else {
      BSPCompressedBundle compMsgBundle = compressor.compressBundle(msg);
      byte[] data = compMsgBundle.getData();
      peer.incrementCounter(
          BSPPeerImpl.PeerCounter.COMPRESSED_BYTES_SENT, data.length);
      return ByteBuffer.wrap(data);
    }
  }

  @Override
  public Iterator<Entry<InetSocketAddress, LinkedList<M>>> getMessageIterator() {
    return this.outgoingQueues.entrySet().iterator();
  }
}
