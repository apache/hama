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
import org.apache.hama.bsp.message.AbstractOutgoingMessageManager;
import org.apache.hama.util.ReflectionUtils;

public class OutgoingVertexMessageManager<M extends Writable> extends
    AbstractOutgoingMessageManager<GraphJobMessage> {
  protected static final Log LOG = LogFactory
      .getLog(OutgoingVertexMessageManager.class);

  private Combiner<Writable> combiner;
  private HashMap<InetSocketAddress, MessagePerVertex> storage = new HashMap<InetSocketAddress, MessagePerVertex>();

  @SuppressWarnings("unchecked")
  @Override
  public void init(HamaConfiguration conf) {
    this.conf = conf;

    final String combinerName = conf.get(Constants.COMBINER_CLASS);
    if (combinerName != null) {
      try {
        combiner = (Combiner<Writable>) ReflectionUtils
            .newInstance(combinerName);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void addMessage(String peerName, GraphJobMessage msg) {
    InetSocketAddress targetPeerAddress = getSocketAddress(peerName);
    if (msg.isVertexMessage()) {
      WritableComparable<?> vertexID = msg.getVertexId();

      if (!storage.containsKey(targetPeerAddress)) {
        storage.put(targetPeerAddress, new MessagePerVertex());
      }

      MessagePerVertex msgPerVertex = storage.get(targetPeerAddress);
      msgPerVertex.add(vertexID, msg);

      // Combining messages
      if (combiner != null && msgPerVertex.get(vertexID).getNumOfValues() > 1) {

        // Overwrite
        storage.get(targetPeerAddress).put(
            vertexID,
            new GraphJobMessage(vertexID, combiner.combine(msgPerVertex.get(
                vertexID).getIterableMessages())));
      }
    } else {
      outgoingBundles.get(targetPeerAddress).addMessage(msg);
    }
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
      }

    };
  }
}
