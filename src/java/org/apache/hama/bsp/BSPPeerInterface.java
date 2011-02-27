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
package org.apache.hama.bsp;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hama.Constants;
import org.apache.zookeeper.KeeperException;

/**
 * BSP communication interface.
 */
public interface BSPPeerInterface extends BSPRPCProtocolVersion, Closeable,
    Constants {

  /**
   * Send a data with a tag to another BSPSlave corresponding to hostname.
   * Messages sent by this method are not guaranteed to be received in a sent
   * order.
   * 
   * @param peerName
   * @param msg
   * @throws IOException
   */
  public void send(String peerName, BSPMessage msg) throws IOException;

  /**
   * Puts a message to local queue.
   * 
   * @param msg
   * @throws IOException
   */
  public void put(BSPMessage msg) throws IOException;

  /**
   * @return A message from the peer's received messages queue (a FIFO).
   * @throws IOException
   */
  public BSPMessage getCurrentMessage() throws IOException;

  /**
   * @return The number of messages in the peer's received messages queue.
   */
  public int getNumCurrentMessages();

  /**
   * Barrier Synchronization.
   * 
   * Sends all the messages in the outgoing message queues to the corresponding
   * remote peers.
   * 
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void sync() throws IOException, KeeperException, InterruptedException;

  /**
   * @return the count of current super-step
   */
  public long getSuperstepCount();

  /**
   * @return The name of this peer in the format "hostname:port".
   */
  public String getPeerName();

  /**
   * @return The names of all the peers executing tasks from the same job
   *         (including this peer).
   */
  public String[] getAllPeerNames();

  /**
   * Clears all queues entries.
   */
  public void clear();
}
