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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;

/**
 * This manager takes care of the messaging. It is responsible to launch a
 * server if needed and deal with incoming data.
 * 
 */
public interface MessageManager<M extends Writable> {

  /**
   * Init can be used to start servers and initialize internal state.
   * 
   * @param conf
   * @param peerAddress
   */
  public void init(Configuration conf, InetSocketAddress peerAddress);

  /**
   * Close is called after a task ran. Should be used to cleanup things e.G.
   * stop a server.
   */
  public void close();

  /**
   * Get the current message.
   * 
   * @return
   * @throws IOException
   */
  public M getCurrentMessage() throws IOException;

  /**
   * Send a message to the peer.
   * 
   * @param peerName
   * @param msg
   * @throws IOException
   */
  public void send(String peerName, M msg) throws IOException;

  /**
   * Returns an iterator of messages grouped by peer.
   * 
   * @return
   */
  public Iterator<Entry<InetSocketAddress, LinkedList<M>>> getMessageIterator();

  /**
   * This is the real transferring to a host with a bundle.
   * 
   * @param addr
   * @param bundle
   * @throws IOException
   */
  public void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
      throws IOException;

  /**
   * Clears the outgoing queue. Can be used to switch queues.
   */
  public void clearOutgoingQueues();

  /**
   * Gets the number of messages in the current queue.
   * 
   * @return
   */
  public int getNumCurrentMessages();

}
