/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * Synchronization Deamon. <br\>
 */
public class SyncServerImpl implements SyncServer, Callable<Long> {

  private static final Log LOG = LogFactory.getLog(SyncServerImpl.class);

  private Configuration conf = new Configuration();
  private Server server;

  private int parties;

  private CyclicBarrier barrier;
  private CyclicBarrier leaveBarrier;
  private Set<Integer> partySet;
  private Set<String> peerAddresses;

  private volatile long superstep = 0L;

  public SyncServerImpl(int parties, String host, int port) throws IOException {
    this.parties = parties;
    this.barrier = new CyclicBarrier(parties);
    this.leaveBarrier = new CyclicBarrier(parties, new SuperStepIncrementor(
        this));

    this.partySet = Collections.synchronizedSet(new HashSet<Integer>(parties));
    // tree set so there is ascending order for consistent returns in
    // getAllPeerNames()
    this.peerAddresses = Collections.synchronizedSet(new TreeSet<String>());
    // allocate ten more rpc handler than parties for additional services to
    // plug in or to deal with failure.
    this.server = RPC.getServer(this, host, port, parties + 10, false, conf);
    LOG.info("Sync Server is now up at: " + host + ":" + port + "!");
  }

  public void start() throws IOException {
    server.start();
  }

  @Override
  public void stopServer() {
    server.stop();
  }

  public void join() throws InterruptedException {
    server.join();
  }

  public static SyncServer getService(Configuration conf)
      throws NumberFormatException, IOException {
    String syncAddress = conf.get("hama.sync.server.address");
    if (syncAddress == null || syncAddress.isEmpty()
        || !syncAddress.contains(":")) {
      throw new IllegalArgumentException(
          "Server sync address must contain a colon and must be non-empty and not-null! Property \"hama.sync.server.address\" was: "
              + syncAddress);
    }
    String[] hostPort = syncAddress.split(":");
    return (SyncServer) RPC.waitForProxy(SyncServer.class,
        SyncServer.versionID,
        new InetSocketAddress(hostPort[0], Integer.valueOf(hostPort[1])), conf);

  }

  @Override
  public void enterBarrier(TaskAttemptID id) {
    LOG.info("Task: " + id.getId() + " entered Barrier!");
    if (partySet.contains(id.getId())) {
      try {
        barrier.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (BrokenBarrierException e) {
        e.printStackTrace();
      }
    } else {
      LOG.warn("TaskID " + id + " is no verified task!");
    }
  }

  @Override
  public void leaveBarrier(TaskAttemptID id) {
    LOG.info("Task: " + id.getId() + " leaves Barrier!");
    if (partySet.contains(id.getId())) {
      try {
        leaveBarrier.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (BrokenBarrierException e) {
        e.printStackTrace();
      }
    } else {
      LOG.warn("TaskID " + id + " is no verified task!");
    }
  }

  @Override
  public synchronized void register(TaskAttemptID id, Text hostAddress,
      LongWritable port) {
    partySet.add(id.getId());
    String peer = hostAddress.toString() + ":" + port.get();
    peerAddresses.add(peer);
    LOG.info("Registered: " + id.getId() + " for peer " + peer);
    if (partySet.size() > parties) {
      LOG.warn("Registered more tasks than configured!");
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return clientVersion;
  }

  private static class SuperStepIncrementor implements Runnable {

    private final SyncServerImpl instance;

    public SuperStepIncrementor(SyncServerImpl syncServer) {
      this.instance = syncServer;
    }

    @Override
    public void run() {
      synchronized (instance) {
        this.instance.superstep += 1L;
        LOG.info("Entering superstep: " + this.instance.superstep);
      }
    }

  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    LOG.info(Arrays.toString(args));
    if (args.length == 3) {
      SyncServerImpl syncServer = new SyncServerImpl(Integer.valueOf(args[0]),
          args[1], Integer.valueOf(args[2]));
      syncServer.start();
      syncServer.join();
    } else {
      throw new IllegalArgumentException(
          "Argument count does not match 3! Given size was " + args.length
              + " and parameters were " + Arrays.toString(args));
    }
  }

  @Override
  public Long call() throws Exception {
    this.start();
    this.join();
    return this.superstep;
  }

  @Override
  public synchronized LongWritable getSuperStep() {
    return new LongWritable(superstep);
  }

  @Override
  public synchronized StringArrayWritable getAllPeerNames() {
    return new StringArrayWritable(
        peerAddresses.toArray(new String[peerAddresses.size()]));
  }

  @Override
  public void deregisterFromBarrier(TaskAttemptID id, Text hostAddress,
      LongWritable port) {
    // TODO Auto-generated method stub
    // basically has to recreate the barriers and remove from the two basic
    // sets.
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    // TODO Auto-generated method stub
    return new ProtocolSignature();
  }

}
