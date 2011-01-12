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

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.zookeeper.KeeperException;

class Client implements Runnable{
  public static final Log LOG = LogFactory.getLog(Client.class);
  private final static List<Result> collector = Collections.synchronizedList(new ArrayList<Result>()); 
 
  public static final class Result{
    private final int num;
    private final int order;
    private final String peer;
    private final String content;

    public Result(final int num, final int order, final String peer, 
                  final String content){
      this.num = num;
      this.order = order;
      this.peer = peer;
      this.content = content;
    }

    public String getContent(){
      return this.content;
    }
  
    public int getOrder(){
      return this.order; 
    }
  
    public int getNumber(){
      return this.num;
    }
  
    public String getPeer(){
      return this.peer;
    }

    public String toString(){
      return " number:"+getNumber() + " order:"+getOrder()+
             " peer:"+getPeer()+" content:"+getContent();
    }
    
  }

  public static class HelloBSP extends BSP {
    private Configuration conf;

    @Override
    public void bsp(BSPPeer bspPeer) throws IOException, KeeperException,
        InterruptedException {
      int cnt = 0;
      Result r = null;
      LOG.info("BSPPeer all peer names -> "+bspPeer.getAllPeerNames());
      LOG.info("Current peer -> "+bspPeer.getPeerName());
      for (String otherPeer : bspPeer.getAllPeerNames()) {
        if (bspPeer.getPeerName().equals(otherPeer)) {
          int num = Integer.parseInt(conf.get("bsp.peers.num"));
          String result = "Hello BSP from " + (cnt + 1) + " of " + num + ": "
              + bspPeer.getPeerName();
          r = new Result(num, cnt, bspPeer.getPeerName(), result);
          LOG.info("(Targeted server) result object -> "+r);
          collector.add(r);
        }
        cnt++;
      }// for
      bspPeer.sync();
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

  public List<Result> getResults(){
    return collector;
  }

  @Override
  public void run(){
    try{
      HamaConfiguration c1 = new HamaConfiguration();
      BSPJob bsp = new BSPJob(c1, Client.class);
      bsp.setJobName("Hello BSP");
      bsp.setBspClass(HelloBSP.class);
      String hamahome = System.getenv("HAMA_HOME");
      if(null == hamahome || "".equals(hamahome)){
        hamahome = System.getProperty("user.dir");
      }
      LOG.info("HAMA_HOME:"+hamahome);
      // e.g. hama-0.2.0-dev-test.jar
      File dir = new File(hamahome+"/build/");
      for(String file: dir.list()){
        boolean flag = file.matches("hama.*test.jar");
        if(flag){ 
          LOG.info("Jar file "+file);
          bsp.setJar(hamahome+"/build/"+file);  // TODO: should not hardcoded!!!
        }
      }
      BSPJobClient client = new BSPJobClient(c1);
      ClusterStatus cluster = client.getClusterStatus(false);
      int groomsize = cluster.getGroomServers();
      assertEquals("Check if GroomServer number matches.", groomsize, TestBSPMaster.GROOM_SIZE);
      bsp.setNumBspTask(groomsize);
      BSPJobClient.runJob(bsp);
    }catch(IOException ioe){
      LOG.info("Error submitting job.", ioe);
    }
  } 
}

