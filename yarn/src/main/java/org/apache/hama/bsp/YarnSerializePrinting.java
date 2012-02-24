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
package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.SyncException;

public class YarnSerializePrinting {

  public static class HelloBSP extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> {
    public static final Log LOG = LogFactory.getLog(HelloBSP.class);
    private final static int PRINT_INTERVAL = 1000;
    private int num;

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> bspPeer)
        throws IOException, SyncException, InterruptedException {
      num = bspPeer.getConfiguration().getInt("bsp.peers.num", 0);
      LOG.info(bspPeer.getAllPeerNames());
      int i = 0;
      for (String otherPeer : bspPeer.getAllPeerNames()) {
        String peerName = bspPeer.getPeerName();
        if (peerName.equals(otherPeer)) {
          LOG.info("Hello BSP from " + (i + 1) + " of " + num + ": " + peerName);
        }

        Thread.sleep(PRINT_INTERVAL);
        bspPeer.sync();
        i++;
      }
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();
    // TODO some keys that should be within a conf
    conf.set("yarn.resourcemanager.address", "0.0.0.0:8040");
    conf.set("bsp.local.dir", "/tmp/bsp-yarn/");

    YARNBSPJob job = new YARNBSPJob(conf);
    job.setBspClass(HelloBSP.class);
    job.setJarByClass(HelloBSP.class);
    job.setJobName("Serialize Printing");
    job.setMemoryUsedPerTaskInMb(50);
    job.setNumBspTask(2);
    job.waitForCompletion(true);
  }
}
