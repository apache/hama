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
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * This manager takes care of the messaging. It is responsible to launch a
 * server if needed and deal with incoming data.
 * 
 */
public interface MessageManager<M extends Writable> {

  public static final String OUTGOING_MESSAGE_MANAGER_CLASS = "hama.messenger.outgoing.message.manager.class";
  public static final String RECEIVE_QUEUE_TYPE_CLASS = "hama.messenger.receive.queue.class";
  public static final String MAX_CACHED_CONNECTIONS_KEY = "hama.messenger.max.cached.connections";

  /**
   * Init can be used to start servers and initialize internal state. If you are
   * implementing a subclass, please call the super version of this method. The
   * socket address provided may be used for initializing the server connection.
   * If it is used or not used, the message manager should provide a unique
   * InetSocketAddress that identifies the server for the peer listening on the
   * socket. This socket address should be returned in
   * {@link MessageManager#getListenerAddress()}
   * 
   */
  public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      HamaConfiguration conf, InetSocketAddress peerAddress);

  /**
   * Close is called after a task ran. Should be used to cleanup things e.G.
   * stop a server.
   */
  public void close();

  /**
   * Get the current message.
   * 
   * @throws IOException
   */
  public M getCurrentMessage() throws IOException;

  /**
   * Send a message to the peer.
   * 
   * @throws IOException
   */
  public void send(String peerName, M msg) throws IOException;

  /**
   * Returns an bundle of messages grouped by peer.
   * 
   */
  public Iterator<Entry<InetSocketAddress, BSPMessageBundle<M>>> getOutgoingBundles();

  /**
   * This is the real transferring to a host with a bundle.
   * 
   */
  public void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
      throws IOException;

  /**
   * Clears the outgoing queue. Can be used to switch queues.
   */
  public void clearOutgoingMessages();

  /**
   * Gets the number of messages in the current queue.
   * 
   */
  public int getNumCurrentMessages();

  /**
   * Send the messages to self to receive in the next superstep.
   */
  public void loopBackMessages(BSPMessageBundle<M> bundle)
      throws IOException;

  /**
   * Send the message to self to receive in the next superstep.
   */
  public void loopBackMessage(Writable message) throws IOException;

  /**
   * Register a listener for the events in message manager.
   * 
   * @param listener <code>MessageEventListener</code> object that processes the
   *          messages sent to remote peer.
   * @throws IOException
   */
  public void registerListener(MessageEventListener<M> listener)
      throws IOException;

  /**
   * Returns the server address on which the incoming connections are listening
   * on.
   */
  public InetSocketAddress getListenerAddress();
}
