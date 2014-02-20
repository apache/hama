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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.message.compress.BSPMessageCompressor;
import org.apache.hama.util.BSPNetUtils;
import org.apache.hama.util.ReflectionUtils;

public class OutgoingPOJOMessageBundle<M extends Writable> implements
    OutgoingMessageManager<M> {

  private HamaConfiguration conf;
  private BSPMessageCompressor<M> compressor;
  private Combiner<M> combiner;
  private final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();
  private HashMap<InetSocketAddress, BSPMessageBundle<M>> outgoingBundles = new HashMap<InetSocketAddress, BSPMessageBundle<M>>();

  @SuppressWarnings("unchecked")
  @Override
  public void init(HamaConfiguration conf, BSPMessageCompressor<M> compressor) {
    this.conf = conf;
    this.compressor = compressor;
    final String combinerName = conf.get(Constants.COMBINER_CLASS);
    if (combinerName != null) {
      try {
        this.combiner = (Combiner<M>) ReflectionUtils.newInstance(conf
            .getClassByName(combinerName));
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Override
  public void addMessage(String peerName, M msg) {
    InetSocketAddress targetPeerAddress = getSocketAddress(peerName);

    if (combiner != null) {
      BSPMessageBundle<M> bundle = outgoingBundles.get(targetPeerAddress);
      bundle.addMessage(msg);
      BSPMessageBundle<M> combined = new BSPMessageBundle<M>();
      combined.setCompressor(compressor,
          conf.getLong("hama.messenger.compression.threshold", 128));
      combined.addMessage(combiner.combine(bundle));
      outgoingBundles.put(targetPeerAddress, combined);
    } else {
      outgoingBundles.get(targetPeerAddress).addMessage(msg);
    }
  }

  private InetSocketAddress getSocketAddress(String peerName) {
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
      bundle.setCompressor(compressor,
          conf.getLong("hama.messenger.compression.threshold", 128));
      outgoingBundles.put(targetPeerAddress, bundle);
    }
    return targetPeerAddress;
  }

  @Override
  public void clear() {
    outgoingBundles.clear();
  }

  @Override
  public Iterator<Entry<InetSocketAddress, BSPMessageBundle<M>>> getBundleIterator() {
    return outgoingBundles.entrySet().iterator();
  }

}
