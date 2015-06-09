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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.message.AbstractOutgoingMessageManager;

public class OutgoingVertexMessageManager<M extends Writable> extends
    AbstractOutgoingMessageManager<GraphJobMessage> {
  protected static final Log LOG = LogFactory
      .getLog(OutgoingVertexMessageManager.class);

  @Override
  public void init(HamaConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void addMessage(String peerName, GraphJobMessage msg) {
    InetSocketAddress targetPeerAddress = getSocketAddress(peerName);
    outgoingBundles.get(targetPeerAddress).addMessage(msg);
  }

  @Override
  public void clear() {
    outgoingBundles.clear();
  }

  @Override
  public Iterator<Entry<InetSocketAddress, BSPMessageBundle<GraphJobMessage>>> getBundleIterator() {
    return outgoingBundles.entrySet().iterator();
  }
}
