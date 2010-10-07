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
package org.apache.hama.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.zookeeper.KeeperException;

public class SerializePrinting {
  
  public static class HelloBSP extends BSP {
    private Configuration conf;

    @Override
    public void bsp(BSPPeer bspPeer) throws IOException, KeeperException,
        InterruptedException {
      int num = Integer.parseInt(conf.get("bsp.peers.num"));

      for (int i = 0; i < num; i++) {
        if (bspPeer.getId() == i) {
          System.out.println("Hello BSP from " + i + " of " + num + ": "
              + bspPeer.getServerName());
        }

        Thread.sleep(100);
        bspPeer.sync();
      }

    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

  }

  public static void main(String[] args) throws InterruptedException,
      IOException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    // Execute locally
    // conf.set("bsp.master.address", "local");

    BSPJob bsp = new BSPJob(conf, SerializePrinting.class);
    // Set the job name
    bsp.setJobName("serialize printing");
    bsp.setBspClass(HelloBSP.class);

    BSPJobClient.runJob(bsp);
  }
}
