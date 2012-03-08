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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

/**
 * BSP communication interface.
 */
public interface BSPPeer<K1, V1, K2, V2, M extends Writable> extends Constants {

  /**
   * Send a data with a tag to another BSPSlave corresponding to hostname.
   * Messages sent by this method are not guaranteed to be received in a sent
   * order.
   * 
   * @param peerName
   * @param msg
   * @throws IOException
   */
  public void send(String peerName, M msg) throws IOException;

  /**
   * @return A message from the peer's received messages queue (a FIFO).
   * @throws IOException
   */
  public M getCurrentMessage() throws IOException;

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
   * @throws IOException
   * @throws SyncException
   * @throws InterruptedException
   */
  public void sync() throws IOException, SyncException, InterruptedException;

  /**
   * @return the count of current super-step
   */
  public long getSuperstepCount();

  /**
   * @return the name of this peer in the format "hostname:port".
   */
  public String getPeerName();

  /**
   * @param index
   * @return the name of n-th peer from sorted array by name.
   */
  public String getPeerName(int index);

  /**
   * @return the names of all the peers executing tasks from the same job
   *         (including this peer).
   */
  public String[] getAllPeerNames();

  /**
   * @return the number of peers
   */
  public int getNumPeers();

  /**
   * Clears all queues entries.
   */
  public void clear();

  /**
   * Writes a key/value pair to the output collector.
   * 
   * @param key your key object
   * @param value your value object
   * @throws IOException
   */
  public void write(K2 key, V2 value) throws IOException;

  /**
   * Deserializes the next input key value into the given objects.
   * 
   * @param key
   * @param value
   * @return false if there are no records to read anymore
   * @throws IOException
   */
  public boolean readNext(K1 key, V1 value) throws IOException;

  /**
   * Reads the next key value pair and returns it as a pair.
   * 
   * @return null if there are no records left.
   * @throws IOException
   */
  public KeyValuePair<K1, V1> readNext() throws IOException;

  /**
   * Closes the input and opens it right away, so that the file pointer is at
   * the beginning again.
   */
  public void reopenInput() throws IOException;

  /**
   * @return the jobs configuration
   */
  public Configuration getConfiguration();
  
  /**
   * Get the {@link Counter} of the given group with the given name.
   * 
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  public Counter getCounter(Enum<?> name);

  /**
   * Get the {@link Counter} of the given group with the given name.
   * 
   * @param group counter group
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
   */
  public Counter getCounter(String group, String name);
  
  /**
   * Increments the counter identified by the key, which can be of
   * any {@link Enum} type, by the specified amount.
   * 
   * @param key key to identify the counter to be incremented. The key can be
   *            be any <code>Enum</code>. 
   * @param amount A non-negative amount by which the counter is to 
   *               be incremented.
   */
  public void incrementCounter(Enum<?> key, long amount);
  
  /**
   * Increments the counter identified by the group and counter name
   * by the specified amount.
   * 
   * @param group name to identify the group of the counter to be incremented.
   * @param counter name to identify the counter within the group.
   * @param amount A non-negative amount by which the counter is to 
   *               be incremented.
   */
  public void incrementCounter(String group, String counter, long amount);
}
