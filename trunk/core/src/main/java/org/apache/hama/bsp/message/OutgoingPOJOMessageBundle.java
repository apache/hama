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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.util.ReflectionUtils;

public class OutgoingPOJOMessageBundle<M extends Writable> extends
    AbstractOutgoingMessageManager<M> {

  private Combiner<M> combiner;

  @SuppressWarnings("unchecked")
  @Override
  public void init(HamaConfiguration conf) {
    this.conf = conf;

    final String combinerName = conf.get(Constants.COMBINER_CLASS);
    if (combinerName != null) {
      try {
        this.combiner = (Combiner<M>) ReflectionUtils.newInstance(combinerName);
      } catch (ClassNotFoundException e) {
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
      combined.addMessage(combiner.combine(bundle));
      outgoingBundles.put(targetPeerAddress, combined);
    } else {
      outgoingBundles.get(targetPeerAddress).addMessage(msg);
    }
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
