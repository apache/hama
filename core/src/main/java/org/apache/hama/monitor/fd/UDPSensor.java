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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

/**
 * Failure detector UDP client.
 */
public class UDPSensor implements Sensor, Callable<Object> {

  public static final Log LOG = LogFactory.getLog(UDPSensor.class);
  /** 
   * The default interval hearbeat.
   */
  private static long HEARTBEAT_INTERVAL; 

  /* UDP server host and port */
  private String host;
  private int port;
  private final DatagramChannel channel;
  private AtomicBoolean running = new AtomicBoolean(false);
  private AtomicLong sequence = new AtomicLong(0);

  private final ExecutorService scheduler;

  /**
   * Constructor for UDP client. Setting up configuration 
   * and open DatagramSocket.
   */
  public UDPSensor(HamaConfiguration configuration){
    this.host = configuration.get("bsp.monitor.fd.udp_host", "localhost");
    this.port = configuration.getInt("bsp.monitor.fd.udp_port", 16384);
    HEARTBEAT_INTERVAL = 
      configuration.getInt("bsp.monitor.fd.heartbeat_interval", 1000);
    DatagramChannel tmp = null;
    try{
      tmp = DatagramChannel.open();
    }catch(IOException ioe){
      LOG.error("Unable to open datagram channel.", ioe);
    }
    this.channel = tmp;
    if(null == this.channel)
      throw new NullPointerException("Fail to open udp channel.");
    this.scheduler = Executors.newSingleThreadExecutor();
  } 


  /**
   * The heartbeat function, signifying its existence.
   */
  @Override
  public void heartbeat() throws IOException{
    ByteBuffer heartbeat = ByteBuffer.allocate(8);
    heartbeat.clear();
    heartbeat.putLong(sequence.incrementAndGet()); 
    heartbeat.flip();
    channel.send(heartbeat, new InetSocketAddress(this.host, this.port));
    if(LOG.isDebugEnabled()){
      LOG.debug("Heartbeat sequence "+sequence.get()+ " is sent to "+this.host+
      ":"+ this.port);
    }
  }

  public String getHost(){
    return this.host;
  }
  
  public int getPort(){
    return this.port;
  }

  public long heartbeatInterval(){
    return HEARTBEAT_INTERVAL;
  }

  @Override
  public Object call() throws Exception {
    while(running.get()){
      try{
        heartbeat();
        Thread.sleep(HEARTBEAT_INTERVAL);
      }catch(InterruptedException ie){
        LOG.error("UDPSensor is interrupted.", ie);
        Thread.currentThread().interrupt();
      }catch(IOException ioe){ 
        LOG.error("Sensor fails in sending heartbeat.", ioe);
      }
    }
    LOG.info("Sensor at "+this.host+" stops sending heartbeat.");
    return null;
  }

  @Override
  public void start() {
    if(!running.compareAndSet(false, true)) {
      throw new IllegalStateException("Sensor is already started."); 
    }
    this.scheduler.submit(this);
  }

  @Override
  public void stop(){
    running.set(false);
    if(null != this.channel) {
      try{ 
        this.channel.socket().close();
        this.channel.close(); 
      }catch(IOException ioe){ 
        LOG.error("Error closing sensor channel.",ioe); 
      }
    }
    this.scheduler.shutdown();
  }

  public boolean isShutdown(){
    return this.channel.socket().isClosed() && !running.get();
  }

}
