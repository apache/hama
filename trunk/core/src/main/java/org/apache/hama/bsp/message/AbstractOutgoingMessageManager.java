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

import java.net.InetSocketAddress;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.util.BSPNetUtils;

public abstract class AbstractOutgoingMessageManager<M extends Writable>
    implements OutgoingMessageManager<M> {

  protected HamaConfiguration conf;
  
  protected final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();
  protected HashMap<InetSocketAddress, BSPMessageBundle<M>> outgoingBundles =  new HashMap<InetSocketAddress, BSPMessageBundle<M>>();

  protected InetSocketAddress getSocketAddress(String peerName) {
    InetSocketAddress targetPeerAddress = null;
    // Get socket for target peer.
    if (peerSocketCache.containsKey(peerName)) {
      targetPeerAddress = peerSocketCache.get(peerName);
    } else {
      targetPeerAddress = BSPNetUtils.getAddress(peerName);
      peerSocketCache.put(peerName, targetPeerAddress);
    }

    if (!outgoingBundles.containsKey(targetPeerAddress)) {
      BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
      outgoingBundles.put(targetPeerAddress, bundle);
    }
    return targetPeerAddress;
  }
}
