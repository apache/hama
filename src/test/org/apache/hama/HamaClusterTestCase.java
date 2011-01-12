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
package org.apache.hama;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.util.ClusterUtil;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.bsp.GroomServer;

public abstract class HamaClusterTestCase extends HamaTestCase {
  public static final Log LOG = LogFactory.getLog(HamaClusterTestCase.class);
  public MiniBSPCluster cluster;
  protected MiniDFSCluster dfsCluster;
  protected MiniZooKeeperCluster zooKeeperCluster;
  protected int groomServers;
  protected boolean startDfs;

  /** default constructor */
  public HamaClusterTestCase() {
    this(1);
  }

  public HamaClusterTestCase(int groomServers) {
    this(groomServers, true, 10);
  }

  public HamaClusterTestCase(int groomServers, boolean startDfs, int threadpool) {
    super();
    this.startDfs = startDfs;
    this.groomServers = groomServers;
    conf.setInt("bsp.test.threadpool", threadpool);
  }

  public MiniBSPCluster getCluster(){
    return this.cluster;
  }

  /**
   * Actually start the MiniBSP instance.
   */
  protected void hamaClusterSetup() throws Exception {
    File testDir = new File(getUnitTestdir(getName()).toString());

    // Note that this is done before we create the MiniHamaCluster because we
    // need to edit the config to add the ZooKeeper servers.
    this.zooKeeperCluster = new MiniZooKeeperCluster();
    int clientPort = this.zooKeeperCluster.startup(testDir);
    conf.set("hama.zookeeper.property.clientPort", Integer.toString(clientPort));

    // start the mini cluster
    this.cluster = new MiniBSPCluster(conf, groomServers);
  }

  @Override
  protected void setUp() throws Exception {
    try {
      if (this.startDfs) {
        // This spews a bunch of warnings about missing scheme. TODO: fix.
        this.dfsCluster = new MiniDFSCluster(0, this.conf, 2, true, true, true,
          null, null, null, null);

        // mangle the conf so that the fs parameter points to the minidfs we
        // just started up
        FileSystem filesystem = dfsCluster.getFileSystem();
        conf.set("fs.defaultFS", filesystem.getUri().toString());
        Path parentdir = filesystem.getHomeDirectory();
        
        filesystem.mkdirs(parentdir);
      }

      // do the super setup now. if we had done it first, then we would have
      // gotten our conf all mangled and a local fs started up.
      super.setUp();

      // start the instance
      hamaClusterSetup();
    } catch (Exception e) {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (zooKeeperCluster != null) {
        zooKeeperCluster.shutdown();
      }
      if (dfsCluster != null) {
        shutdownDfs(dfsCluster);
      }
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    try {
      if (this.cluster != null) {
        try {
          
          this.cluster.shutdown();
        } catch (Exception e) {
          LOG.warn("Closing mini dfs", e);
        }
        try {
          this.zooKeeperCluster.shutdown();
        } catch (IOException e) {
          LOG.warn("Shutting down ZooKeeper cluster", e);
        }
      }
      if (startDfs) {
        shutdownDfs(dfsCluster);
      }
    } catch (Exception e) {
      LOG.error(e);
    }
  }


  /**
   * Use this utility method debugging why cluster won't go down.  On a
   * period it throws a thread dump.  Method ends when all cluster
   * groomservers and master threads are no long alive.
   */
  public void threadDumpingJoin() {
    if (this.cluster.getGroomServerThreads() != null) {
      for(Thread t: this.cluster.getGroomServerThreads()) {
        threadDumpingJoin(t);
      }
    }
    threadDumpingJoin(this.cluster.getMaster());
  }

  protected void threadDumpingJoin(final Thread t) {
    if (t == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (t.isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Continuing...", e);
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        startTime = System.currentTimeMillis();
        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }
}
