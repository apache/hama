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
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.compress.BSPCompressedBundle;
import org.apache.hama.util.LRUCache;

public final class AvroMessageManagerImpl<M extends Writable> extends
    CompressableMessageManager<M> implements Sender<M> {

  private NettyServer server = null;

  // also cache the senders, getting a new sender from a transceiver generates
  // exceptions
  private final HashMap<InetSocketAddress, Sender<M>> peers = new HashMap<InetSocketAddress, Sender<M>>();
  private LRUCache<InetSocketAddress, NettyTransceiver> peersLRUCache;

  @SuppressWarnings("serial")
  @Override
  public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      Configuration conf, InetSocketAddress addr) {
    super.init(attemptId, peer, conf, addr);
    super.initCompression(conf);
    server = new NettyServer(new SpecificResponder(Sender.class, this), addr);
    peersLRUCache = new LRUCache<InetSocketAddress, NettyTransceiver>(
        maxCachedConnections) {
      @Override
      protected final boolean removeEldestEntry(
          Map.Entry<InetSocketAddress, NettyTransceiver> eldest) {
        if (size() > this.capacity) {
          NettyTransceiver client = eldest.getValue();
          client.close();
          peers.remove(eldest.getKey());
          return true;
        }
        return false;
      }
    };
  }

  @Override
  public void close() {
    super.close();
    server.close();
  }

  public void put(BSPMessageBundle<M> messages) throws IOException {
    this.loopBackMessages(messages);
  }

  @Override
  public void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
      throws IOException {
    AvroBSPMessageBundle<M> msg = new AvroBSPMessageBundle<M>();
    msg.setData(serializeMessage(bundle));
    Sender<M> sender = getSender(addr);
    sender.transfer(msg);
  }

  /**
   * @param addr, socket address to which BSP Peer Connection will be
   *          established
   * @return BSP Peer Connection, tried to return cached connection, else
   *         returns a new connection and caches it
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private final Sender<M> getSender(InetSocketAddress addr) throws IOException {
    NettyTransceiver client = peersLRUCache.get(addr);
    if (client == null) {
      client = new NettyTransceiver(addr);
      Sender<M> sender = SpecificRequestor.getClient(Sender.class, client);
      peersLRUCache.put(addr, client);
      peers.put(addr, sender);
    }
    return peers.get(addr);
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

  private final BSPMessageBundle<M> deserializeMessage(ByteBuffer buffer)
      throws IOException {
    BSPMessageBundle<M> msg = new BSPMessageBundle<M>();
    byte[] byteArray = buffer.array();
    if (compressor == null) {
      peer.incrementCounter(BSPPeerImpl.PeerCounter.MESSAGE_BYTES_RECEIVED,
          byteArray.length);
      ByteArrayInputStream inArray = new ByteArrayInputStream(byteArray);
      DataInputStream in = new DataInputStream(inArray);
      msg.readFields(in);
    } else {
      peer.incrementCounter(BSPPeerImpl.PeerCounter.COMPRESSED_BYTES_RECEIVED,
          byteArray.length);
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
      peer.incrementCounter(BSPPeerImpl.PeerCounter.MESSAGE_BYTES_TRANSFERED,
          byteArray.length);
      return ByteBuffer.wrap(byteArray);
    } else {
      BSPCompressedBundle compMsgBundle = compressor.compressBundle(msg);
      byte[] data = compMsgBundle.getData();
      peer.incrementCounter(BSPPeerImpl.PeerCounter.COMPRESSED_BYTES_SENT,
          data.length);
      return ByteBuffer.wrap(data);
    }
  }
}
