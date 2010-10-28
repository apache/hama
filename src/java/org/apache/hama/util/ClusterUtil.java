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
package org.apache.hama.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.bsp.GroomServer;
import org.apache.log4j.Logger;

public class ClusterUtil {
  static final Logger LOG = Logger.getLogger(ClusterUtil.class);
  /**
   * Data Structure to hold GroomServer Thread and GroomServer instance
   */
  public static class GroomServerThread extends Thread {
    private final GroomServer groomServer;

    public GroomServerThread(final GroomServer r, final int index) {
      super(r, "GroomServer:" + index);
      this.groomServer = r;
    }

    /** @return the groom server */
    public GroomServer getGroomServer() {
      return this.groomServer;
    }

    /**
     * Block until the groom server has come online, indicating it is ready
     * to be used.
     */
    public void waitForServerOnline() {
      while (!groomServer.isRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // continue waiting
        }
      }
    }
  }

  /**
   * Creates a {@link GroomServerThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hrsc Class to create.
   * @param index Used distingushing the object returned.
   * @throws IOException
   * @return Groom server added.
   */
  public static ClusterUtil.GroomServerThread createGroomServerThread(final Configuration c,
    final Class<? extends GroomServer> hrsc, final int index)
  throws IOException {
    GroomServer server;
      try {
        server = hrsc.getConstructor(Configuration.class).newInstance(c);
      } catch (Exception e) {
        IOException ioe = new IOException();
        ioe.initCause(e);
        throw ioe;
      }
      return new ClusterUtil.GroomServerThread(server, index);
  }

  /**
   * Start the cluster.
   * @param m
   * @param conf 
   * @param groomservers
   * @return Address to use contacting master.
   * @throws InterruptedException 
   * @throws IOException 
   */
  public static String startup(final BSPMaster m,
      final List<ClusterUtil.GroomServerThread> groomservers, Configuration conf) throws IOException, InterruptedException {
    if (m != null) {
      BSPMaster.startMaster((HamaConfiguration) conf);
    }

    if (groomservers != null) {
      for (ClusterUtil.GroomServerThread t: groomservers) {
        t.start();
      }
    }
    
    return m == null? null: BSPMaster.getAddress(conf).getHostName();
  }

  public static void shutdown(BSPMaster master,
      List<GroomServerThread> groomThreads, Configuration conf) {
    LOG.debug("Shutting down HAMA Cluster");
    // TODO: 
  }
}
