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
import java.util.HashMap;

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
import org.apache.hama.util.CompressionUtil;

/**
 * Implementation of the {@link HadoopMessageManager}.
 * 
 */
public final class HadoopMessageManagerImpl<M extends Writable> extends
    CompressableMessageManager<M> implements HadoopMessageManager<M> {

  private static final Log LOG = LogFactory
      .getLog(HadoopMessageManagerImpl.class);

  private final HashMap<InetSocketAddress, HadoopMessageManager<M>> peers = new HashMap<InetSocketAddress, HadoopMessageManager<M>>();

  private Server server = null;

  @Override
  public final void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
      Configuration conf, InetSocketAddress peerAddress) {
    super.init(attemptId, peer, conf, peerAddress);
    super.initCompression(conf);
    startRPCServer(conf, peerAddress);
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
      if (compressor != null) {
        BSPCompressedBundle compMsgBundle = compressor.compressBundle(bundle);
        if (CompressionUtil.getCompressionRatio(compMsgBundle, bundle) < 1.0) {
          bspPeerConnection.put(compMsgBundle);
        } else {
          bspPeerConnection.put(bundle);
        }
      } else {
        bspPeerConnection.put(bundle);
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected final HadoopMessageManager<M> getBSPPeerConnection(
      InetSocketAddress addr) throws IOException {
    HadoopMessageManager<M> peer = peers.get(addr);
    if (peer == null) {
      peer = (HadoopMessageManager<M>) RPC.getProxy(HadoopMessageManager.class,
          HadoopMessageManager.versionID, addr, this.conf);
      this.peers.put(addr, peer);
    }
    return peer;
  }

  @Override
  public final void put(M msg) {
    this.localQueueForNextIteration.add(msg);
    peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED, 1L);
  }

  @Override
  public final void put(BSPMessageBundle<M> messages) {
    for (M message : messages.getMessages()) {
      this.localQueueForNextIteration.add(message);
    }
  }

  @Override
  public final void put(BSPCompressedBundle compMsgBundle) {
    BSPMessageBundle<M> bundle = compressor.decompressBundle(compMsgBundle);
    for (M message : bundle.getMessages()) {
      this.localQueueForNextIteration.add(message);
    }
  }

  @Override
  public final long getProtocolVersion(String arg0, long arg1)
      throws IOException {
    return versionID;
  }

}
