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
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.ipc.HamaRPCProtocolVersion;
import org.apache.hama.ipc.RPC;
import org.apache.hama.ipc.RPC.Server;
import org.apache.hama.util.LRUCache;

/**
 * Implementation of the {@link HamaMessageManager}.
 * 
 */
public final class HamaMessageManagerImpl<M extends Writable> extends
    AbstractMessageManager<M> implements HamaMessageManager<M> {

  private static final Log LOG = LogFactory
      .getLog(HamaMessageManagerImpl.class);

  private Server server;

  private LRUCache<InetSocketAddress, HamaMessageManager<M>> peersLRUCache = null;

  @SuppressWarnings("serial")
  @Override
  public final void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      HamaConfiguration conf, InetSocketAddress peerAddress) {
    super.init(attemptId, peer, conf, peerAddress);
    startRPCServer(conf, peerAddress);
    peersLRUCache = new LRUCache<InetSocketAddress, HamaMessageManager<M>>(
        maxCachedConnections) {
      @Override
      protected final boolean removeEldestEntry(
          Map.Entry<InetSocketAddress, HamaMessageManager<M>> eldest) {
        if (size() > this.capacity) {
          HamaMessageManager<M> proxy = eldest.getValue();
          RPC.stopProxy(proxy);
          return true;
        }
        return false;
      }
    };
  }

  private final void startRPCServer(Configuration conf,
      InetSocketAddress peerAddress) {
    try {
      startServer(peerAddress.getHostName(), peerAddress.getPort());
    } catch (IOException ioe) {
      LOG.error("Fail to start RPC server!", ioe);
      throw new RuntimeException("RPC Server could not be launched!");
    }
  }

  private void startServer(String hostName, int port) throws IOException {
    int retry = 0;
    try {
      this.server = RPC.getServer(this, hostName, port,
          conf.getInt("hama.default.messenger.handler.threads.num", 5), false,
          conf);

      server.start();
      LOG.info("BSPPeer address:" + server.getListenerAddress().getHostName()
          + " port:" + server.getListenerAddress().getPort());
    } catch (BindException e) {
      LOG.warn("Address already in use. Retrying " + hostName + ":" + port + 1);
      startServer(hostName, port + 1);
      retry++;

      if (retry > 5) {
        throw new RuntimeException("RPC Server could not be launched!");
      }
    }
  }

  @Override
  public final void close() {
    super.close();
    if (server != null) {
      server.stop();
    }
  }

  @Override
  public final void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
      throws IOException {
    HamaMessageManager<M> bspPeerConnection = this.getBSPPeerConnection(addr);
    if (bspPeerConnection == null) {
      throw new IllegalArgumentException("Can not find " + addr.toString()
          + " to transfer messages to!");
    } else {
      peer.incrementCounter(BSPPeerImpl.PeerCounter.MESSAGE_BYTES_TRANSFERED, bundle.getLength());
      bspPeerConnection.put(bundle);
    }
  }

  /**
   * @param addr, socket address to which BSP Peer Connection will be
   *          established
   * @return BSP Peer Connection, tried to return cached connection, else
   *         returns a new connection and caches it
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  protected final HamaMessageManager<M> getBSPPeerConnection(
      InetSocketAddress addr) throws IOException {
    HamaMessageManager<M> bspPeerConnection;
    if (!peersLRUCache.containsKey(addr)) {
      bspPeerConnection = (HamaMessageManager<M>) RPC.getProxy(
          HamaMessageManager.class, HamaRPCProtocolVersion.versionID, addr,
          this.conf);
      peersLRUCache.put(addr, bspPeerConnection);
    } else {
      bspPeerConnection = peersLRUCache.get(addr);
    }
    return bspPeerConnection;
  }

  @Override
  public final void put(M msg) throws IOException {
    loopBackMessage(msg);
  }

  @Override
  public final void put(BSPMessageBundle<M> messages) throws IOException {
    loopBackMessages(messages);
  }

  @Override
  public final long getProtocolVersion(String arg0, long arg1)
      throws IOException {
    return versionID;
  }

  @Override
  public InetSocketAddress getListenerAddress() {
    if (this.server != null) {
      return this.server.getListenerAddress();
    }
    return null;
  }

}
