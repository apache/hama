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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public abstract class HamaClusterTestCase extends HamaTestCase {
  public static final Log LOG = LogFactory.getLog(HamaClusterTestCase.class);
  protected MiniDFSCluster dfsCluster;
  protected MiniZooKeeperCluster zooKeeperCluster;
  protected boolean startDfs;

  /** default constructor */
  public HamaClusterTestCase() {
    this(false);
  }

  public HamaClusterTestCase(boolean startDfs) {
    super();
    this.startDfs = startDfs;
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
      if (startDfs) {
        shutdownDfs(dfsCluster);
      }
    } catch (Exception e) {
      LOG.error(e);
    }
  }
}
