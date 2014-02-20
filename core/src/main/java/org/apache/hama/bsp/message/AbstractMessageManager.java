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
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.compress.BSPMessageCompressor;
import org.apache.hama.bsp.message.compress.BSPMessageCompressorFactory;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.apache.hama.bsp.message.queue.MemoryQueue;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.message.queue.SingleLockQueue;
import org.apache.hama.bsp.message.queue.SynchronizedQueue;
import org.apache.hama.util.ReflectionUtils;

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

  protected OutgoingMessageManager<M> outgoingMessageManager;
  protected MessageQueue<M> localQueue;

  // this must be a synchronized implementation: this is accessed per RPC
  protected SynchronizedQueue<M> localQueueForNextIteration;
  // this peer object is just used for counter incrementation
  protected BSPPeer<?, ?, ?, ?, M> peer;

  // the task attempt id
  protected TaskAttemptID attemptId;

  // to maximum cached connections in the concrete message manager
  protected int maxCachedConnections = 100;

  // List of listeners for all the sent messages
  protected Queue<MessageEventListener<M>> messageListenerQueue;

  protected BSPMessageCompressor<M> compressor;

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#init(org.apache.hama.bsp.
   * TaskAttemptID, org.apache.hama.bsp.BSPPeer,
   * org.apache.hadoop.conf.Configuration, java.net.InetSocketAddress)
   */
  @Override
  public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      HamaConfiguration conf, InetSocketAddress peerAddress) {
    this.messageListenerQueue = new LinkedList<MessageEventListener<M>>();
    this.attemptId = attemptId;
    this.peer = peer;
    this.conf = conf;
    this.localQueue = getReceiverQueue();
    this.localQueueForNextIteration = getSynchronizedReceiverQueue();
    this.maxCachedConnections = conf.getInt(MAX_CACHED_CONNECTIONS_KEY, 100);

    this.compressor = new BSPMessageCompressorFactory<M>().getCompressor(conf);
    this.outgoingMessageManager = getOutgoingMessageManager();
    this.outgoingMessageManager.init(conf, compressor);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#close()
   */
  @Override
  public void close() {
    try {
      outgoingMessageManager.clear();
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
  public final void clearOutgoingMessages() {
    outgoingMessageManager.clear();

    if (conf.getBoolean(MessageQueue.PERSISTENT_QUEUE, false)
        && localQueue.size() > 0) {

      if (localQueue.isMemoryBasedQueue()
          && localQueueForNextIteration.isMemoryBasedQueue()) {

        // To reduce the number of element additions
        if (localQueue.size() > localQueueForNextIteration.size()) {
          localQueue.addAll(localQueueForNextIteration);
        } else {
          localQueueForNextIteration.addAll(localQueue);
          localQueue = localQueueForNextIteration.getMessageQueue();
        }

      } else {

        // TODO find the way to switch disk-based queue efficiently.
        localQueueForNextIteration.addAll(localQueue);
        if (localQueue != null) {
          localQueue.close();
        }
        localQueue = localQueueForNextIteration.getMessageQueue();

      }
    } else {
      if (localQueue != null) {
        localQueue.close();
      }

      localQueue = localQueueForNextIteration.getMessageQueue();
    }

    localQueue.prepareRead();
    localQueueForNextIteration = getSynchronizedReceiverQueue();
    notifyInit();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#send(java.lang.String,
   * org.apache.hadoop.io.Writable)
   */
  @Override
  public void send(String peerName, M msg) throws IOException {
    outgoingMessageManager.addMessage(peerName, msg);
    peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_SENT, 1L);
    notifySentMessage(peerName, msg);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.message.MessageManager#getMessageIterator()
   */
  @Override
  public final Iterator<Entry<InetSocketAddress, BSPMessageBundle<M>>> getOutgoingBundles() {
    return this.outgoingMessageManager.getBundleIterator();
  }

  protected OutgoingMessageManager<M> getOutgoingMessageManager() {
    @SuppressWarnings("unchecked")
    OutgoingMessageManager<M> messageManager = ReflectionUtils.newInstance(conf
        .getClass(MessageManager.OUTGOING_MESSAGE_MANAGER_CLASS,
            OutgoingPOJOMessageBundle.class, OutgoingMessageManager.class));
    return messageManager;
  }

  /**
   * Returns a new queue implementation based on what was configured. If nothing
   * has been configured for "hama.messenger.queue.class" then the
   * {@link MemoryQueue} is used. If you have scalability issues, then better
   * use {@link DiskQueue}.
   * 
   * @return a <b>new</b> queue implementation.
   */
  protected MessageQueue<M> getReceiverQueue() {
    @SuppressWarnings("unchecked")
    MessageQueue<M> queue = ReflectionUtils.newInstance(conf.getClass(
        MessageManager.RECEIVE_QUEUE_TYPE_CLASS, MemoryQueue.class,
        MessageQueue.class));
    queue.init(conf, attemptId);
    return queue;
  }

  protected SynchronizedQueue<M> getSynchronizedReceiverQueue() {
    return SingleLockQueue.synchronize(getReceiverQueue());
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
    for (MessageEventListener<M> aMessageListenerQueue : this.messageListenerQueue) {
      aMessageListenerQueue.onMessageSent(peerName, message);
    }
  }

  private void notifyReceivedMessage(M message) throws IOException {
    for (MessageEventListener<M> aMessageListenerQueue : this.messageListenerQueue) {
      aMessageListenerQueue.onMessageReceived(message);
    }
  }

  private void notifyInit() {
    for (MessageEventListener<M> aMessageListenerQueue : this.messageListenerQueue) {
      aMessageListenerQueue.onInitialized();
    }
  }

  private void notifyClose() {
    for (MessageEventListener<M> aMessageListenerQueue : this.messageListenerQueue) {
      aMessageListenerQueue.onClose();
    }
  }

  @Override
  public void registerListener(MessageEventListener<M> listener)
      throws IOException {
    if (listener != null)
      this.messageListenerQueue.add(listener);

  }

  @Override
  public void loopBackMessages(BSPMessageBundle<M> bundle) throws IOException {
    bundle.setCompressor(compressor,
        conf.getLong("hama.messenger.compression.threshold", 128));

    Iterator<? extends Writable> it = bundle.iterator();
    while (it.hasNext()) {
      loopBackMessage(it.next());
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
