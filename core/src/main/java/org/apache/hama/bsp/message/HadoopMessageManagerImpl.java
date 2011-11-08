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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.util.BSPNetUtils;

/**
 * Implementation of the {@link HadoopMessageManager}.
 * 
 */
public class HadoopMessageManagerImpl implements MessageManager,
    HadoopMessageManager {

  private static final Log LOG = LogFactory
      .getLog(HadoopMessageManagerImpl.class);

  private Server server = null;
  private Configuration conf;

  private final HashMap<InetSocketAddress, HadoopMessageManager> peers = new HashMap<InetSocketAddress, HadoopMessageManager>();
  private final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();

  private final HashMap<InetSocketAddress, LinkedList<BSPMessage>> outgoingQueues = new HashMap<InetSocketAddress, LinkedList<BSPMessage>>();
  private Deque<BSPMessage> localQueue = new LinkedList<BSPMessage>();
  // this must be a synchronized implementation: this is accessed per RPC
  private final ConcurrentLinkedQueue<BSPMessage> localQueueForNextIteration = new ConcurrentLinkedQueue<BSPMessage>();

  @Override
  public void init(Configuration conf, InetSocketAddress peerAddress) {
    this.conf = conf;
    startRPCServer(conf, peerAddress);
  }

  private void startRPCServer(Configuration conf, InetSocketAddress peerAddress) {
    try {
      this.server = RPC.getServer(this, peerAddress.getHostName(),
          peerAddress.getPort(), conf);
      server.start();
      LOG.info(" BSPPeer address:" + peerAddress.getHostName() + " port:"
          + peerAddress.getPort());
    } catch (IOException e) {
      LOG.error("Fail to start RPC server!", e);
      throw new RuntimeException("RPC Server could not be launched!");
    }
  }

  @Override
  public void close() {
    if (server != null) {
      server.stop();
    }
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

  @Override
  public Iterator<Entry<InetSocketAddress, LinkedList<BSPMessage>>> getMessageIterator() {
    return this.outgoingQueues.entrySet().iterator();
  }

  protected HadoopMessageManager getBSPPeerConnection(InetSocketAddress addr)
      throws IOException {
    HadoopMessageManager peer = peers.get(addr);
    if (peer == null) {
      peer = (HadoopMessageManager) RPC.getProxy(HadoopMessageManager.class,
          HadoopMessageManager.versionID, addr, this.conf);
      this.peers.put(addr, peer);
    }
    return peer;
  }

  @Override
  public void transfer(InetSocketAddress addr, BSPMessageBundle bundle)
      throws IOException {

    HadoopMessageManager bspPeerConnection = this.getBSPPeerConnection(addr);

    if (bspPeerConnection == null) {
      throw new IllegalArgumentException("Can not find " + addr.toString()
          + " to transfer messages to!");
    } else {
      bspPeerConnection.put(bundle);
    }
  }

  @Override
  public void clearOutgoingQueues() {
    this.outgoingQueues.clear();
    localQueue.addAll(localQueueForNextIteration);
    localQueueForNextIteration.clear();
  }

  @Override
  public void put(BSPMessage msg) {
    this.localQueueForNextIteration.add(msg);
  }

  @Override
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
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return versionID;
  }

}
