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

package org.apache.hama.monitor.fd;

import static java.util.concurrent.TimeUnit.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;

/**
 * Test case for Phi accrual fail detector. 
 */
public class TestFD extends HamaCluster {
  public static final Log LOG = LogFactory.getLog(TestFD.class);
  final HamaConfiguration conf;
  final ScheduledExecutorService sched;

  public TestFD() {
    this.conf = getConf();
    this.sched = Executors.newScheduledThreadPool(10);
  }

  public void setUp() throws Exception { }

  /**
   * Test Phi Accrual Fialure Detector.
   */
  public void testCumulativeDistributedFunction() throws Exception {
    this.conf.setInt("bsp.monitor.fd.udp_port", 9765);
    UDPSupervisor server = new UDPSupervisor(this.conf);
    UDPSensor client = new UDPSensor(this.conf); 
    this.sched.schedule(server, 0, SECONDS);
    this.sched.schedule(client, 2, SECONDS);
    boolean flag = true;
    int count = 0;
    while(flag){
      count++;
      Thread.sleep(1000*3);
      double phi = server.suspicionLevel("127.0.0.1");
      if(LOG.isDebugEnabled())
        LOG.debug("Phi value:"+phi+" Double.isInfinite(phi):"+Double.isInfinite(phi));
      assertTrue("In normal case phi should not go infinity!", !Double.isInfinite(phi));
      if(10 < count){
        flag = false;
      }
    }
    client.shutdown();
    server.shutdown();
    LOG.info("Finished testing suspicion level.");
  }

  /**
   * Test when sensor fails.
   */
  public void testSensorFailure() throws Exception{
    this.conf.setInt("bsp.monitor.fd.udp_port", 2874);
    UDPSupervisor server = new UDPSupervisor(this.conf);
    UDPSensor client = new UDPSensor(this.conf); 
    this.sched.schedule(server, 0, SECONDS);
    this.sched.schedule(client, 2, SECONDS);
    int count = 0;
    boolean flag = true;
    while(flag){
      count++;
      double phi = server.suspicionLevel("127.0.0.1");
      Thread.sleep(1000*3);
      if(5 < count){
        client.shutdown(); 
        Thread.sleep(1000*4);
        phi = server.suspicionLevel("127.0.0.1");
        if(LOG.isDebugEnabled())
          LOG.debug("Phi value should go infinity:"+Double.isInfinite(phi));
        assertTrue("In normal case phi should not go infinity!", Double.isInfinite(phi));
      }
      if(10 < count){
        flag = false;
      }
    }
    server.shutdown();
    LOG.info("Finished testing client failure case.");
  }
  
  public void tearDown() throws Exception { 
    sched.shutdown();
  }
}
