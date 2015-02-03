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
package org.apache.hama.graph;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.message.OutgoingMessageManager;
import org.apache.hama.bsp.message.compress.BSPMessageCompressor;
import org.apache.hama.util.BSPNetUtils;

public class OutgoingVertexMessageManager<M extends Writable> implements
    OutgoingMessageManager<GraphJobMessage> {
  protected static final Log LOG = LogFactory
      .getLog(OutgoingVertexMessageManager.class);

  private HamaConfiguration conf;
  private BSPMessageCompressor<GraphJobMessage> compressor;
  private Combiner<Writable> combiner;
  private final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();
  private HashMap<InetSocketAddress, BSPMessageBundle<GraphJobMessage>> outgoingBundles = new HashMap<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>();

  private HashMap<InetSocketAddress, MessagePerVertex> storage = new HashMap<InetSocketAddress, MessagePerVertex>();

  @SuppressWarnings("unchecked")
  @Override
  public void init(HamaConfiguration conf,
      BSPMessageCompressor<GraphJobMessage> compressor) {
    this.conf = conf;
    this.compressor = compressor;
    if (!conf.getClass(Constants.COMBINER_CLASS, Combiner.class).equals(
        Combiner.class)) {
      LOG.debug("Combiner class: " + conf.get(Constants.COMBINER_CLASS));

      combiner = (Combiner<Writable>) org.apache.hadoop.util.ReflectionUtils
          .newInstance(conf.getClass(Constants.COMBINER_CLASS, Combiner.class),
              conf);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void addMessage(String peerName, GraphJobMessage msg) {
    InetSocketAddress targetPeerAddress = getSocketAddress(peerName);

    if (msg.isVertexMessage()) {
      WritableComparable vertexID = msg.getVertexId();

      if (!storage.containsKey(targetPeerAddress)) {
        storage.put(targetPeerAddress, new MessagePerVertex());
      }

      MessagePerVertex msgPerVertex = storage.get(targetPeerAddress);
      msgPerVertex.add(vertexID, msg.getVertexValue());

      // Combining messages
      if (combiner != null
          && msgPerVertex.get(vertexID).getVertexValue().size() > 1) {
        storage.get(targetPeerAddress).put(
            vertexID,
            new GraphJobMessage(vertexID, combiner.combine(msgPerVertex.get(
                vertexID).getVertexValue())));
      }

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
      BSPMessageBundle<GraphJobMessage> bundle = new BSPMessageBundle<GraphJobMessage>();
      if (conf.getBoolean("hama.messenger.runtime.compression", false)) {
        bundle.setCompressor(compressor,
            conf.getLong("hama.messenger.compression.threshold", 128));
      }
      outgoingBundles.put(targetPeerAddress, bundle);
    }
    return targetPeerAddress;
  }

  @Override
  public void clear() {
    outgoingBundles.clear();
    storage.clear();
  }

  @Override
  public Iterator<Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>> getBundleIterator() {
    return new Iterator<Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>>() {
      final Iterator<Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>> bundles = outgoingBundles
          .entrySet().iterator();

      @Override
      public boolean hasNext() {
        return bundles.hasNext();
      }

      @Override
      public Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>> next() {
        Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>> bundle = bundles
            .next();

        MessagePerVertex msgStorage = storage.get(bundle.getKey());
        if (msgStorage != null) {
          Iterator<GraphJobMessage> it = msgStorage.iterator();
          while (it.hasNext()) {
            bundle.getValue().addMessage(it.next());
          }
        }

        storage.remove(bundle.getKey());
        return bundle;
      }

      @Override
      public void remove() {
        // TODO Auto-generated method stub
      }

    };
  }

}
