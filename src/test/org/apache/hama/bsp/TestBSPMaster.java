/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;

public class TestBSPMaster extends HamaCluster {

  private Log LOG = LogFactory.getLog(TestBSPMaster.class);

  private HamaConfiguration conf;
  private BSPMaster bspm;
  private List<GroomServer> groomServers;
  public static final int GROOM_SIZE = 2;
  public static final int THREAD_POOL = 20;
  //public static final int DFS_PORT = 9000;

  public TestBSPMaster() throws Exception{
    super(GROOM_SIZE, THREAD_POOL); 
    this.conf = getConf();
  }

  public void setUp() throws Exception {
    super.setUp();
    this.bspm = getCluster().getBSPMaster();
    this.groomServers = getCluster().getGroomServers();
    assertNotNull("Ensure BSP Master instance exists.", bspm);
    assertNotNull("Ensure GroomServers exist.", this.groomServers);
    int c=0;
    while(GROOM_SIZE > this.bspm.groomServerStatusKeySet().size()){
      LOG.info("Waiting for GroomServer registering to BSPMaster."); 
      try{
        Thread.sleep(1000); 
        c++;
        if(10<c){
          fail("Waiting too long for GroomServers' registeration. ");
        }
      }catch(InterruptedException e){
        Thread.currentThread().interrupt();
      }
    }
  }

  private static String currentAddress() throws UnknownHostException{
    InetAddress addr = InetAddress.getLocalHost();
    return addr.getHostAddress();
  }

  private boolean contains(String nic){
    for(GroomServerStatus sts: this.bspm.groomServerStatusKeySet()){
      if(sts.getPeerName().equals(nic)){
        return true;
      }
    }
    return false;
  }

  public void testBSPMasterGroomServerNexus() throws Exception{
    // registeration
    assertTrue("Assert GroomServer exists.", 
               this.bspm.groomServerStatusKeySet().size() > 0);
    for(GroomServer groom: this.groomServers){
      String nic = groom.getBspPeerName();
      assertTrue("Check if registered groom server exists.", contains(nic));
    }

    final ScheduledExecutorService sched = getCluster().getScheduler();
    LOG.info("Start submiting job ...");

    // client submit job 
    Client c = new Client();
    sched.schedule(c, 0, SECONDS);
    int cnt = 0;
    while(c.getResults().isEmpty()) {
      try{
        Thread.sleep(1000);
        cnt++;
        if(10 < cnt){
          fail("Can not get client submitted job result.");
        }
      }catch(InterruptedException ie){
        LOG.error("Fail to check client result.", ie);
        Thread.currentThread().interrupt();
      }
    }
    List<Client.Result> results = c.getResults();
    LOG.info("Task results size:"+results.size());
    assertEquals("Ensure return collection size is 2.", results.size(), GROOM_SIZE);
    for(Client.Result r: results){
      LOG.info("Collected result => "+r);
      assertEquals("Check result.", r.getContent(), 
                   "Hello BSP from " + (r.getOrder() + 1) + " of " +  
                   r.getNumber() + ": " + r.getPeer());
    }
    LOG.info("Finish executing test nexus method.");
  }

  public void tearDown() throws Exception{
    super.tearDown();
  }
}
