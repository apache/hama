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
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.BSPPeerImpl;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.compress.BSPCompressedBundle;
import org.apache.hama.ipc.HamaRPCProtocolVersion;
import org.apache.hama.util.LRUCache;

/**
 * Implementation of the {@link HadoopMessageManager}.
 * 
 */
public final class HadoopMessageManagerImpl<M extends Writable> extends
    CompressableMessageManager<M> implements HadoopMessageManager<M> {

  private static final Log LOG = LogFactory
      .getLog(HadoopMessageManagerImpl.class);

  private Server server = null;

  private LRUCache<InetSocketAddress, HadoopMessageManager<M>> peersLRUCache = null;

  @SuppressWarnings("serial")
  @Override
  public final void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      Configuration conf, InetSocketAddress peerAddress) {
    super.init(attemptId, peer, conf, peerAddress);
    super.initCompression(conf);
    startRPCServer(conf, peerAddress);
    peersLRUCache = new LRUCache<InetSocketAddress, HadoopMessageManager<M>>(
        maxCachedConnections) {
      @Override
      protected final boolean removeEldestEntry(
          Map.Entry<InetSocketAddress, HadoopMessageManager<M>> eldest) {
        if (size() > this.capacity) {
          HadoopMessageManager<M> proxy = eldest.getValue();
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
      this.server = RPC.getServer(this, peerAddress.getHostName(),
          peerAddress.getPort(), conf);
      server.start();
      LOG.info(" BSPPeer address:" + peerAddress.getHostName() + " port:"
          + peerAddress.getPort());
    } catch (IOException e) {
      LOG.error("Fail to start RPC server!", e);
      throw new RuntimeException("RPC Server could not be launched!");
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
    HadoopMessageManager<M> bspPeerConnection = this.getBSPPeerConnection(addr);
    if (bspPeerConnection == null) {
      throw new IllegalArgumentException("Can not find " + addr.toString()
          + " to transfer messages to!");
    } else {
      if (compressor != null
          && (bundle.getApproximateSize() > conf.getLong(
              "hama.messenger.compression.threshold", 1048576))) {
        BSPCompressedBundle compMsgBundle = compressor.compressBundle(bundle);
        bspPeerConnection.put(compMsgBundle);
        peer.incrementCounter(BSPPeerImpl.PeerCounter.COMPRESSED_MESSAGES, 1L);
      } else {
        bspPeerConnection.put(bundle);
      }
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
  protected final HadoopMessageManager<M> getBSPPeerConnection(
      InetSocketAddress addr) throws IOException {
    HadoopMessageManager<M> bspPeerConnection;
    if (!peersLRUCache.containsKey(addr)) {
      bspPeerConnection = (HadoopMessageManager<M>) RPC.getProxy(
          HadoopMessageManager.class, HamaRPCProtocolVersion.versionID, addr,
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
  public final void put(BSPCompressedBundle compMsgBundle) throws IOException {
    BSPMessageBundle<M> bundle = compressor.decompressBundle(compMsgBundle);
    loopBackMessages(bundle);
  }

  @Override
  public final long getProtocolVersion(String arg0, long arg1)
      throws IOException {
    return versionID;
  }

}
