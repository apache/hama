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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

public class OutgoingVertexMessagesManager<M extends Writable> implements
    OutgoingMessageManager<GraphJobMessage> {
  protected static final Log LOG = LogFactory
      .getLog(OutgoingVertexMessagesManager.class);

  private HamaConfiguration conf;
  private BSPMessageCompressor<GraphJobMessage> compressor;
  private Combiner<Writable> combiner;
  private final HashMap<String, InetSocketAddress> peerSocketCache = new HashMap<String, InetSocketAddress>();
  private HashMap<InetSocketAddress, BSPMessageBundle<GraphJobMessage>> outgoingBundles = new HashMap<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>();

  @SuppressWarnings("rawtypes")
  private HashMap<InetSocketAddress, Map<WritableComparable, Writable>> vertexMessageMap = new HashMap<InetSocketAddress, Map<WritableComparable, Writable>>();
  private List<Writable> tmp;

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

    if (msg.isVertexMessage() && combiner != null) {
      WritableComparable vertexID = msg.getVertexId();
      Writable vertexValue = msg.getVertexValue();

      if (!vertexMessageMap.containsKey(targetPeerAddress)) {
        vertexMessageMap.put(targetPeerAddress,
            new HashMap<WritableComparable, Writable>());
      }

      Map<WritableComparable, Writable> combinedMessage = vertexMessageMap
          .get(targetPeerAddress);

      if (combinedMessage.containsKey(vertexID)) {
        tmp = new ArrayList<Writable>();
        tmp.add(combinedMessage.get(vertexID));
        tmp.add(vertexValue);

        Iterable<Writable> iterable = new Iterable<Writable>() {
          @Override
          public Iterator<Writable> iterator() {
            return tmp.iterator();
          }
        };

        combinedMessage.put(vertexID, combiner.combine(iterable));
      } else {
        combinedMessage.put(vertexID, vertexValue);
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
      bundle.setCompressor(compressor, conf.getLong("hama.messenger.compression.threshold", 128));
      outgoingBundles.put(targetPeerAddress, bundle);
    }
    return targetPeerAddress;
  }

  @Override
  public void clear() {
    outgoingBundles.clear();
    vertexMessageMap.clear();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Iterator<Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>> getBundleIterator() {
    if (combiner != null) {
      for (Map.Entry<InetSocketAddress, Map<WritableComparable, Writable>> e : vertexMessageMap
          .entrySet()) {
        for (Map.Entry<WritableComparable, Writable> v : e.getValue()
            .entrySet()) {
          outgoingBundles.get(e.getKey()).addMessage(
              new GraphJobMessage(v.getKey(), v.getValue()));
        }
      }
    }

    vertexMessageMap.clear();
    return outgoingBundles.entrySet().iterator();
  }

}
