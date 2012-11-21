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
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.apache.hama.bsp.message.queue.MemoryQueue;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.message.queue.SingleLockQueue;
import org.apache.hama.bsp.message.queue.SynchronizedQueue;
import org.apache.hama.util.BSPNetUtils;

/**
 * Abstract baseclass that should contain all information and services needed
 * for the concrete RPC subclasses. For example it manages how the queues are
 * managed and it maintains a cache for socket addresses.
 */
public abstract class AbstractMessageManager<M extends Writable> implements
    MessageManager<M>, Configurable {

  protected static final Log LOG = LogFactory
      .getLog(AbstractMessageManager.class);

  // conf is injected via reflection of the factory
  protected Configuration conf;
  protected final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();
  protected final HashMap<InetSocketAddress, MessageQueue<M>> outgoingQueues = new HashMap<InetSocketAddress, MessageQueue<M>>();
  protected MessageQueue<M> localQueue;
  // this must be a synchronized implementation: this is accessed per RPC
  protected SynchronizedQueue<M> localQueueForNextIteration;
  // this peer object is just used for counter incrementation
  protected BSPPeer<?, ?, ?, ?, M> peer;
  // the peer address of this peer
  protected InetSocketAddress peerAddress;
  // the task attempt id
  protected TaskAttemptID attemptId;

  // to maximum cached connections in the concrete message manager
  protected int maxCachedConnections = 100;

  // List of listeners for all the sent messages
  protected Queue<MessageEventListener<M>> messageListenerQueue;

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#init(org.apache.hama.bsp.
   * TaskAttemptID, org.apache.hama.bsp.BSPPeer,
   * org.apache.hadoop.conf.Configuration, java.net.InetSocketAddress)
   */
  @Override
  public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      Configuration conf, InetSocketAddress peerAddress) {
    this.messageListenerQueue = new LinkedList<MessageEventListener<M>>();
    this.attemptId = attemptId;
    this.peer = peer;
    this.conf = conf;
    this.peerAddress = peerAddress;
    this.localQueue = getQueue();
    this.localQueueForNextIteration = getSynchronizedQueue();
    this.maxCachedConnections = conf.getInt(MAX_CACHED_CONNECTIONS_KEY, 100);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#close()
   */
  @Override
  public void close() {
    try {
      Collection<MessageQueue<M>> values = outgoingQueues.values();
      for (MessageQueue<M> msgQueue : values) {
        msgQueue.close();
      }
      localQueue.close();
      // remove possible disk queues from the path
      try {
        FileSystem.get(conf).delete(
            DiskQueue.getQueueDir(conf, attemptId,
                conf.get(DiskQueue.DISK_QUEUE_PATH_KEY)), true);
      } catch (IOException e) {
        LOG.warn("Queue dir couldn't be deleted");
      }
    } finally {
      notifyClose();
    }

  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#finishSendPhase()
   */
  @Override
  public void finishSendPhase() throws IOException {
    Collection<MessageQueue<M>> values = outgoingQueues.values();
    for (MessageQueue<M> msgQueue : values) {
      msgQueue.prepareRead();
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#getCurrentMessage()
   */
  @Override
  public final M getCurrentMessage() throws IOException {
    return localQueue.poll();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#getNumCurrentMessages()
   */
  @Override
  public final int getNumCurrentMessages() {
    return localQueue.size();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#clearOutgoingQueues()
   */
  @Override
  public final void clearOutgoingQueues() {
    localQueue = localQueueForNextIteration.getMessageQueue();
    localQueue.prepareRead();
    localQueueForNextIteration = getSynchronizedQueue();
    notifyInit();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#send(java.lang.String,
   * org.apache.hadoop.io.Writable)
   */
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
    MessageQueue<M> queue = outgoingQueues.get(targetPeerAddress);
    if (queue == null) {
      queue = getQueue();
    }
    queue.add(msg);
    peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_SENT, 1L);
    outgoingQueues.put(targetPeerAddress, queue);
    notifySentMessage(peerName, msg);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#getMessageIterator()
   */
  @Override
  public final Iterator<Entry<InetSocketAddress, MessageQueue<M>>> getMessageIterator() {
    return this.outgoingQueues.entrySet().iterator();
  }

  /**
   * Returns a new queue implementation based on what was configured. If nothing
   * has been configured for "hama.messenger.queue.class" then the
   * {@link MemoryQueue} is used. If you have scalability issues, then better
   * use {@link DiskQueue}.
   * 
   * @return a <b>new</b> queue implementation.
   */
  protected MessageQueue<M> getQueue() {
    Class<?> queueClass = conf.getClass(QUEUE_TYPE_CLASS, MemoryQueue.class);
    LOG.debug("Creating new " + queueClass);
    @SuppressWarnings("unchecked")
    MessageQueue<M> newInstance = (MessageQueue<M>) ReflectionUtils
        .newInstance(queueClass, conf);
    newInstance.init(conf, attemptId);
    return newInstance;
  }

  protected SynchronizedQueue<M> getSynchronizedQueue() {
    return SingleLockQueue.synchronize(getQueue());
  }

  @Override
  public final Configuration getConf() {
    return conf;
  }

  @Override
  public final void setConf(Configuration conf) {
    this.conf = conf;
  }

  private void notifySentMessage(String peerName, M message) {
    Iterator<MessageEventListener<M>> iterator = this.messageListenerQueue
        .iterator();
    while (iterator.hasNext()) {
      iterator.next().onMessageSent(peerName, message);
    }
  }

  private void notifyReceivedMessage(M message) throws IOException {
    Iterator<MessageEventListener<M>> iterator = this.messageListenerQueue
        .iterator();
    while (iterator.hasNext()) {
      iterator.next().onMessageReceived(message);
    }
  }

  private void notifyInit() {
    Iterator<MessageEventListener<M>> iterator = this.messageListenerQueue
        .iterator();
    while (iterator.hasNext()) {
      iterator.next().onInitialized();
    }
  }

  private void notifyClose() {
    Iterator<MessageEventListener<M>> iterator = this.messageListenerQueue
        .iterator();
    while (iterator.hasNext()) {
      iterator.next().onClose();
    }
  }

  @Override
  public void registerListener(MessageEventListener<M> listener)
      throws IOException {
    if (listener != null)
      this.messageListenerQueue.add(listener);

  }

  @Override
  public void loopBackMessages(BSPMessageBundle<? extends Writable> bundle)
      throws IOException {
    for (Writable message : bundle.getMessages()) {
      loopBackMessage(message);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void loopBackMessage(Writable message) throws IOException {
    this.localQueueForNextIteration.add((M) message);
    peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED, 1L);
    notifyReceivedMessage((M) message);

  }

}
