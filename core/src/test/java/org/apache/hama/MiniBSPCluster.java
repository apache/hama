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

import java.io.IOException;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.*;

import static junit.framework.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.bsp.GroomServer;
import org.apache.hama.HamaConfiguration;


public class MiniBSPCluster {

  public static final Log LOG = LogFactory.getLog(MiniBSPCluster.class);

  private ScheduledExecutorService scheduler;

  private HamaConfiguration configuration;
  private BSPMasterRunner master;
  private List<GroomServerRunner> groomServerList = 
    new CopyOnWriteArrayList<GroomServerRunner>();
  private int grooms;

  public class BSPMasterRunner implements Runnable{
    BSPMaster bspm;
    HamaConfiguration conf;

    public BSPMasterRunner(HamaConfiguration conf){
      this.conf = conf;
      if(null == this.conf) 
        throw new NullPointerException("No Configuration for BSPMaster.");
    }  

    public void run(){
      try{
        LOG.info("Starting BSP Master.");
        this.bspm = BSPMaster.startMaster(this.conf); 
        this.bspm.offerService();
      }catch(IOException ioe){
        LOG.error("Fail to startup BSP Master.", ioe);
      }catch(InterruptedException ie){
        LOG.error("BSP Master fails in offerService().", ie);
        Thread.currentThread().interrupt();
      }
    }

    public void shutdown(){
      if(null != this.bspm) this.bspm.shutdown();
    }

    public boolean isRunning(){
      if(null == this.bspm) return false;

      if(this.bspm.currentState().equals(BSPMaster.State.RUNNING)){
        return true;
      } 
      return false;
    }

    public BSPMaster getMaster(){
      return this.bspm;
    }
  }

  public class GroomServerRunner implements Runnable{
    GroomServer gs;
    HamaConfiguration conf;

    public GroomServerRunner(HamaConfiguration conf){
      this.conf = conf;
    }
 
    public void run(){
      try{
        this.gs = GroomServer.constructGroomServer(GroomServer.class, conf);
        GroomServer.startGroomServer(this.gs).join();
      }catch(InterruptedException ie){
        LOG.error("Fail to start GroomServer. ", ie);
        Thread.currentThread().interrupt();
      }
    }

    public void shutdown(){
      try{
        if(null != this.gs) this.gs.shutdown();
      }catch(IOException ioe){
        LOG.info("Fail to shutdown GroomServer.", ioe);
      }
    }
    
    public boolean isRunning(){
      if(null == this.gs) return false;
      return this.gs.isRunning(); 
    }

    public GroomServer getGroomServer(){
      return this.gs;
    }
  }

  public MiniBSPCluster(HamaConfiguration conf, int groomServers) {
    this.configuration = conf;
    this.grooms = groomServers;
    if(1 > this.grooms) {
      this.grooms = 2;  
    }
    LOG.info("Groom server number "+this.grooms);
    int threadpool = conf.getInt("bsp.test.threadpool", 10);
    LOG.info("Thread pool value "+threadpool);
    scheduler = Executors.newScheduledThreadPool(threadpool);
  }

  public void startBSPCluster(){
    startMaster();
    startGroomServers();
  }

  public void shutdownBSPCluster(){
    if(null != this.master && this.master.isRunning())
      this.master.shutdown();
    if(0 < groomServerList.size()){
      for(GroomServerRunner groom: groomServerList){
        if(groom.isRunning()) groom.shutdown();
      }
    }
  }


  public void startMaster(){
    if(null == this.scheduler) 
      throw new NullPointerException("No ScheduledExecutorService exists.");
    this.master = new BSPMasterRunner(this.configuration);
    scheduler.schedule(this.master, 0, SECONDS);
  }

  public void startGroomServers(){
    if(null == this.scheduler) 
      throw new NullPointerException("No ScheduledExecutorService exists.");
    if(null == this.master) 
      throw new NullPointerException("No BSPMaster exists.");
    int cnt=0;
    while(!this.master.isRunning()){
      LOG.info("Waiting BSPMaster up.");
      try{
        Thread.sleep(1000);
        cnt++;
        if(100 < cnt){
          fail("Fail to launch BSPMaster.");
        }
      }catch(InterruptedException ie){
        LOG.error("Fail to check BSP Master's state.", ie);
        Thread.currentThread().interrupt();
      }
    }
    for(int i=0; i < this.grooms; i++){
      HamaConfiguration c = new HamaConfiguration(this.configuration);
      randomPort(c);
      GroomServerRunner gsr = new GroomServerRunner(c);
      groomServerList.add(gsr);
      scheduler.schedule(gsr, 0, SECONDS);
      cnt = 0;
      while(!gsr.isRunning()){
        LOG.info("Waitin for GroomServer up.");
        try{
          Thread.sleep(1000);
          cnt++;
          if(10 < cnt){
            fail("Fail to launch groom server.");
          }
        }catch(InterruptedException ie){
          LOG.error("Fail to check Groom Server's state.", ie);
          Thread.currentThread().interrupt();
        }
      }
    }

  }

  private void randomPort(HamaConfiguration conf){
    try{
      ServerSocket skt = new ServerSocket(0);
      int p = skt.getLocalPort(); 
      skt.close();
      conf.set(Constants.PEER_PORT, new Integer(p).toString());
      conf.setInt(Constants.GROOM_RPC_PORT, p+100);
    }catch(IOException ioe){
      LOG.error("Can not find a free port for BSPPeer.", ioe);
    }
  }

  public void shutdown() {
    shutdownBSPCluster();
    scheduler.shutdown();
  }

  public List<Thread> getGroomServerThreads() {
    List<Thread> list = new ArrayList<Thread>();
    for(GroomServerRunner gsr: groomServerList){
      list.add(new Thread(gsr));
    }
    return list;
  }

  public Thread getMaster() {
    return new Thread(this.master);
  }

  public List<GroomServer> getGroomServers(){
    List<GroomServer> list = new ArrayList<GroomServer>();
    for(GroomServerRunner gsr: groomServerList){
      list.add(gsr.getGroomServer());
    }
    return list;
  }

  public BSPMaster getBSPMaster(){
    if(null != this.master)
      return this.master.getMaster();
    return null;
  }

  public ScheduledExecutorService getScheduler(){
    return this.scheduler;
  }
}
