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
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPPeer;
import org.apache.zookeeper.KeeperException;

public class PiEstimator {
  public static class MyEstimator extends BSP {
    public static final Log LOG = LogFactory.getLog(MyEstimator.class);
    private Configuration conf;
    private static final int iterations = 10000;
    
    @Override
    public void bsp(BSPPeer bspPeer) throws IOException, KeeperException,
        InterruptedException {
      int in = 0, out = 0;
      for (int i = 0; i < iterations; i++) {
        double x = 2.0 * Math.random() - 1.0, y = 2.0 * Math.random() - 1.0;
        if ((Math.sqrt(x * x + y * y) < 1.0)) {
          in++;
        } else {
          out++;
        }
      }

      byte[] tagName = Bytes.toBytes(getName().toString());
      byte[] myData = Bytes.toBytes(4.0 * (double) in / (double) iterations);
      BSPMessage estimate = new BSPMessage(tagName, myData);

      bspPeer.send(new InetSocketAddress("localhost", 61000), estimate);
      bspPeer.sync();

      double pi = 0.0;
      BSPMessage received;
      while ((received = bspPeer.getCurrentMessage()) != null) {
        LOG.info("Receives messages:" + Bytes.toDouble(received.getData()));
        pi = (pi + Bytes.toDouble(received.getData())) / 2;
      }

      if (pi != 0.0) {
        LOG.info("\nEstimated value of PI is " + pi);
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
    //conf.set("bsp.master.address", "local");

    BSPJob bsp = new BSPJob(conf, PiEstimator.class);
    // Set the job name
    bsp.setJobName("pi estimation example");
    bsp.setBspClass(MyEstimator.class);

    BSPJobClient.runJob(bsp);
  }
}
