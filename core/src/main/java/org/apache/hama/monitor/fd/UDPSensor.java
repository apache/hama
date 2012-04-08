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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;

/**
 * Failure detector UDP client.
 */
public class UDPSensor implements Sensor, Runnable{

  public static final Log LOG = LogFactory.getLog(UDPSensor.class);
  /** 
   * The default interval hearbeat.
   */
  private static long HEARTBEAT_INTERVAL; 

  /* UDP server address and port */
  private String address;
  private int port;
  private DatagramChannel channel;
  private AtomicBoolean running = new AtomicBoolean(false);
  private AtomicLong sequence = new AtomicLong(0);

  /**
   * Constructor for UDP client. Setting up configuration 
   * and open DatagramSocket.
   */
  public UDPSensor(Configuration configuration){
    this.address = 
      ((HamaConfiguration)configuration).get("bsp.monitor.fd.udp_address", "localhost");
    this.port = 
      ((HamaConfiguration)configuration).getInt("bsp.monitor.fd.udp_port", 16384);
    HEARTBEAT_INTERVAL = ((HamaConfiguration)configuration).getInt(
      "bsp.monitor.fd.heartbeat_interval", 100);
    running.set(true);
    try{
      channel = DatagramChannel.open();
    }catch(IOException ioe){
      LOG.error("Fail to initialize udp channel.", ioe);
    }
  } 


  /**
   * The heartbeat function, signifying its existence.
   */
  public void heartbeat() throws IOException{
    ByteBuffer heartbeat = ByteBuffer.allocate(8);
    heartbeat.clear();
    heartbeat.putLong(sequence.incrementAndGet()); 
    heartbeat.flip();
    channel.send(heartbeat, new InetSocketAddress(InetAddress.getByName(
      this.address), this.port));
    if(LOG.isDebugEnabled()){
      LOG.debug("Heartbeat sequence "+sequence.get()+ " is sent to "+this.address+":"+ this.port);
    }
  }

  public String getAddress(){
    return this.address;
  }
  
  public int getPort(){
    return this.port;
  }

  public long heartbeatInterval(){
    return HEARTBEAT_INTERVAL;
  }

  public void run(){
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
    LOG.info("Sensor at "+this.address+" stops sending heartbeat.");
  }

  public void shutdown(){
    running.set(false);
    if(null != this.channel) {
      try{ 
        this.channel.socket().close();
        this.channel.close(); 
      }catch(IOException ioe){ 
        LOG.error("Error closing sensor channel.",ioe); 
      }
    }
  }


  public boolean isShutdown(){
    return this.channel.socket().isClosed() && !running.get();
  }

}
