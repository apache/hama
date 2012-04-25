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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.util.BSPNetUtils;

public abstract class AbstractMessageManager<M extends Writable> implements
    MessageManager<M>, Configurable {

  private static final Log LOG = LogFactory
      .getLog(AbstractMessageManager.class);

  // conf is injected via reflection of the factory
  protected Configuration conf;
  protected final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();
  protected final HashMap<InetSocketAddress, MessageQueue<M>> outgoingQueues = new HashMap<InetSocketAddress, MessageQueue<M>>();
  protected MessageQueue<M> localQueue;
  // this must be a synchronized implementation: this is accessed per RPC
  protected SynchronizedQueue<M> localQueueForNextIteration;
  protected BSPPeer<?, ?, ?, ?, M> peer;
  // the peer address of this peer
  protected InetSocketAddress peerAddress;
  // the task attempt id
  protected TaskAttemptID attemptId;

  @Override
  public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      Configuration conf, InetSocketAddress peerAddress) {
    this.attemptId = attemptId;
    this.peer = peer;
    this.conf = conf;
    this.peerAddress = peerAddress;
    localQueue = getQueue();
    localQueue.init(conf, attemptId);
    localQueueForNextIteration = getSynchronizedQueue();
    localQueueForNextIteration.init(conf, attemptId);
  }

  @Override
  public void close() {
    Collection<MessageQueue<M>> values = outgoingQueues.values();
    for (MessageQueue<M> msgQueue : values) {
      msgQueue.close();
    }
    localQueue.close();
  }

  @Override
  public void finishSendPhase() throws IOException {
    Collection<MessageQueue<M>> values = outgoingQueues.values();
    for (MessageQueue<M> msgQueue : values) {
      msgQueue.prepareRead();
    }
  }

  @Override
  public final M getCurrentMessage() throws IOException {
    return localQueue.poll();
  }

  @Override
  public final int getNumCurrentMessages() {
    return localQueue.size();
  }

  @Override
  public final void clearOutgoingQueues() {
    this.outgoingQueues.clear();
    localQueueForNextIteration.prepareRead();
    localQueue.prepareWrite();
    localQueue.addAll(localQueueForNextIteration.getMessageQueue());
    localQueue.prepareRead();
    localQueueForNextIteration.clear();
  }

  @Override
  public void send(String peerName, M msg) throws IOException {
    LOG.debug("Send message (" + msg.toString() + ") to " + peerName);
    InetSocketAddress targetPeerAddress = null;
    // Get socket for target peer.
    if (peerSocketCache.containsKey(peerName)) {
      targetPeerAddress = peerSocketCache.get(peerName);
    } else {
      targetPeerAddress = BSPNetUtils.getAddress(peerName);
      peerSocketCache.put(peerName, targetPeerAddress);
    }
    MessageQueue<M> queue = outgoingQueues.get(targetPeerAddress);
    if (queue == null) {
      queue = getQueue();
    }
    queue.add(msg);
    peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_SENT, 1L);
    outgoingQueues.put(targetPeerAddress, queue);
  }

  @Override
  public final Iterator<Entry<InetSocketAddress, MessageQueue<M>>> getMessageIterator() {
    return this.outgoingQueues.entrySet().iterator();
  }

  /**
   * Returns a new queue implementation based on what was configured. If nothing
   * has been configured for "hama.messenger.queue.class" then the
   * {@link DiskQueue} is used.
   * 
   * @return a <b>new</b> queue implementation.
   */
  protected MessageQueue<M> getQueue() {
    Class<?> queueClass = conf.getClass(QUEUE_TYPE_CLASS, DiskQueue.class);
    @SuppressWarnings("unchecked")
    MessageQueue<M> newInstance = (MessageQueue<M>) ReflectionUtils
        .newInstance(queueClass, conf);
    newInstance.init(conf, attemptId);
    return newInstance;
  }

  protected SynchronizedQueue<M> getSynchronizedQueue() {
    return SynchronizedQueue.synchronize(getQueue());
  }

  public final Configuration getConf() {
    return conf;
  }

  public final void setConf(Configuration conf) {
    this.conf = conf;
  }

}
