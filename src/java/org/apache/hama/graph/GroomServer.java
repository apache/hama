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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.HamaMaster;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.ipc.HeartbeatResponse;
import org.apache.hama.ipc.InterTrackerProtocol;

public class GroomServer implements Runnable {
  public static final Log LOG = LogFactory.getLog(GroomServer.class);

  static {
    Configuration.addDefaultResource("groomserver-default.xml");
  }

  static enum State {
    NORMAL, COMPUTE, SYNC, BARRIER, STALE, INTERRUPTED, DENIED
  };

  HamaConfiguration conf;

  volatile boolean running = true;
  volatile boolean shuttingDown = false;
  boolean justInited = true;

  String groomserverName;
  String localHostname;

  InetSocketAddress masterAddr;
  InterTrackerProtocol jobClient;
  BSPPeer bspPeer;

  short heartbeatResponseId = -1;
  private volatile int heartbeatInterval = 3 * 1000;

  private LocalDirAllocator localDirAllocator;
  Path systemDirectory = null;
  FileSystem systemFS = null;

  public GroomServer(HamaConfiguration conf) throws IOException {
    this.conf = conf;
    masterAddr = HamaMaster.getAddress(conf);

    FileSystem local = FileSystem.getLocal(conf);
    this.localDirAllocator = new LocalDirAllocator("hama.groomserver.local.dir");

    initialize();
  }

  synchronized void initialize() throws IOException {
    if (this.conf.get("slave.host.name") != null) {
      this.localHostname = conf.get("slave.host.name");
    }

    if (localHostname == null) {
      this.localHostname = DNS.getDefaultHost(conf.get(
          "hama.groomserver.dns.interface", "default"), conf.get(
          "hama.groomserver.dns.nameserver", "default"));
    }

    checkLocalDirs(conf.getStrings("hama.groomserver.local.dir"));
    deleteLocalFiles("groomserver");

    this.groomserverName = "groomserver_" + localHostname;
    LOG.info("Starting tracker " + this.groomserverName);

    DistributedCache.purgeCache(this.conf);

    this.jobClient = (InterTrackerProtocol) RPC.waitForProxy(
        InterTrackerProtocol.class, InterTrackerProtocol.versionID, masterAddr,
        conf);
    this.running = true;
    // this.bspPeer = new BSPPeer(this.conf);
  }

  private static void checkLocalDirs(String[] localDirs)
      throws DiskErrorException {
    boolean writable = false;

    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          DiskChecker.checkDir(new File(localDirs[i]));
          writable = true;
        } catch (DiskErrorException e) {
          LOG.warn("Graph Processor local " + e.getMessage());
        }
      }
    }

    if (!writable)
      throw new DiskErrorException("all local directories are not writable");
  }

  public String[] getLocalDirs() {
    return conf.getStrings("hama.groomserver.local.dir");
  }

  public void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getLocal(this.conf).delete(new Path(localDirs[i]));
    }
  }

  public void deleteLocalFiles(String subdir) throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getLocal(this.conf).delete(new Path(localDirs[i], subdir));
    }
  }

  public void cleanupStorage() throws IOException {
    deleteLocalFiles();
  }

  private void startCleanupThreads() throws IOException {

  }

  public State offerService() throws Exception {
    long lastHeartbeat = 0;

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();

        long waitTime = heartbeatInterval - (now - lastHeartbeat);
        if (waitTime > 0) {
          // sleeps for the wait time
          Thread.sleep(waitTime);
        }

        if (justInited) {
          String dir = jobClient.getSystemDir();
          if (dir == null) {
            throw new IOException("Failed to get system directory");
          }
          systemDirectory = new Path(dir);
          systemFS = systemDirectory.getFileSystem(conf);
        }

        // Send the heartbeat and process the jobtracker's directives
        HeartbeatResponse heartbeatResponse = transmitHeartBeat(now);

        // Note the time when the heartbeat returned, use this to decide when to
        // send the
        // next heartbeat
        lastHeartbeat = System.currentTimeMillis();

        justInited = false;
      } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return State.INTERRUPTED;
      } catch (DiskErrorException de) {
        String msg = "Exiting task tracker for disk error:\n"
            + StringUtils.stringifyException(de);
        LOG.error(msg);

        return State.STALE;
      } catch (RemoteException re) {
        return State.DENIED;
      } catch (Exception except) {
        String msg = "Caught exception: "
            + StringUtils.stringifyException(except);
        LOG.error(msg);
      }
    }

    return State.NORMAL;
  }

  private class WalkerLauncher extends Thread {
    // TODO:
  }

  private HeartbeatResponse transmitHeartBeat(long now) throws IOException {
    HeartbeatResponse heartbeatResponse = jobClient
        .heartbeat(heartbeatResponseId);
    return heartbeatResponse;
  }

  public void run() {
    try {
      startCleanupThreads();
      boolean denied = false;
      while (running && !shuttingDown && !denied) {
        boolean staleState = false;
        try {
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception e) {
              if (!shuttingDown) {
                LOG.info("Lost connection to GraphProcessor [" + masterAddr
                    + "].  Retrying...", e);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
          }
        } finally {
          // close();
        }

        if (shuttingDown) {
          return;
        }
        LOG.warn("Reinitializing local state");
        initialize();
      }
    } catch (IOException ioe) {
      LOG.error("Got fatal exception while reinitializing TaskTracker: "
          + StringUtils.stringifyException(ioe));
      return;
    }
  }

  public synchronized void shutdown() throws IOException {
    shuttingDown = true;
    close();
  }

  public synchronized void close() throws IOException {
    this.running = false;

    cleanupStorage();

    // shutdown RPC connections
    RPC.stopProxy(jobClient);
  }

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(GroomServer.class, args, LOG);
    if (args.length != 1) {
      System.out.println("usage: GroomServer");
      System.exit(-1);
    }

    try {
      HamaConfiguration conf = new HamaConfiguration();
      new GroomServer(conf).run();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
