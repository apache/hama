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
package org.apache.hama.checkpoint;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import static java.util.concurrent.TimeUnit.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer.BSPSerializableMessage;
import org.apache.hama.GroomServerRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is responsible for checkpointing messages to hdfs. 
 */
public final class Checkpointer implements Callable {

  public static Log LOG = LogFactory.getLog(Checkpointer.class);

  private final ScheduledExecutorService scheduler = 
    Executors.newScheduledThreadPool(1);
  private final FileSystem dfs; 
  private final AtomicBoolean ckptState = new AtomicBoolean(false);
  private final BSPMessageDeserializer messageDeserializer;
  private final AtomicReference<ScheduledFuture> future =  
    new AtomicReference<ScheduledFuture>(); 

  /** 
   * Reading from socket inputstream as DataInput.
   */
  public static final class BSPMessageDeserializer implements Callable {
    final BlockingQueue<BSPSerializableMessage> messageQueue = 
      new LinkedBlockingQueue<BSPSerializableMessage>();
    final ScheduledExecutorService sched; 
    final ScheduledExecutorService workers; 
    final AtomicBoolean serializerState = new AtomicBoolean(false);
    final ServerSocket server;
    
    public BSPMessageDeserializer(final int port) throws IOException { 
      this.sched = Executors.newScheduledThreadPool(1);
      this.workers = Executors.newScheduledThreadPool(10);
      this.server = new ServerSocket(port); 
      LOG.info("Deserializer's port is opened at "+port);
    }

    public int port() {
      return this.server.getLocalPort(); 
    }

    public void start() {
      if(!serializerState.compareAndSet(false, true)) {
        throw new IllegalStateException("BSPMessageDeserializer has been "+
        "started up.");
      }
      this.sched.schedule(this, 0, SECONDS);
      LOG.info("BSPMessageDeserializer is started.");
    }

    public void stop() {
      try {
        this.server.close();
      } catch(IOException ioe) {
        LOG.error("Unable to close message serializer server socket.", ioe);
      }
      this.sched.shutdown();
      this.workers.shutdown();
      this.serializerState.set(false);
      LOG.info("BSPMessageDeserializer is stopped.");
    }

    public boolean state(){
      return this.serializerState.get();
    }

    /**
     * Message is enqueued for communcating data sent from BSPPeer.
     */
    public BlockingQueue<BSPSerializableMessage> messageQueue() {
      return this.messageQueue;
    }

    public Object call() throws Exception {
      try {
        while(state()) {
          Socket connection = server.accept();
          final DataInput in = new DataInputStream(connection.getInputStream());
          this.workers.schedule(new Callable() {
            public Object call() throws Exception {
              BSPSerializableMessage tmp = new BSPSerializableMessage();
              tmp.readFields(in);
              messageQueue().put(tmp);
              return null;
            }
          }, 0, SECONDS);
        }
      } catch(EOFException eofe) {
        LOG.info("Closing checkpointer's input stream.", eofe);
      } catch(IOException ioe) {
        LOG.error("Error when reconstructing BSPSerializableMessage.", ioe);
      }
      return null;
    }
  }

  public Checkpointer(final Configuration conf) throws IOException {
    this.dfs = FileSystem.get(conf); 
    if(null == this.dfs) 
      throw new NullPointerException("HDFS instance not found.");
    int port = conf.getInt("bsp.checkpoint.port", 
      Integer.parseInt(CheckpointRunner.DEFAULT_PORT));
    if(LOG.isDebugEnabled()) 
      LOG.debug("Checkpoint port value:"+port); 
    this.messageDeserializer = new BSPMessageDeserializer(port);
  }

  /**
   * Activate the checkpoint thread.
   */
  public void start(){
    if(!ckptState.compareAndSet(false, true)) {
      throw new IllegalStateException("Checkpointer has been started up.");
    }
    this.messageDeserializer.start();
    this.future.set(this.scheduler.schedule(this, 0, SECONDS));
    LOG.info("Checkpointer is started.");
  }

  /**
   * Stop checkpoint thread.
   */
  public void stop(){
    this.messageDeserializer.stop();
    this.scheduler.shutdown();
    this.ckptState.set(false);
    LOG.info("Checkpointer is stopped.");
  }
  
  /**
   * Check if checkpointer is running.
   * @return true if checkpointer is runing; false otherwise.
   */
  public boolean isAlive(){
    return !this.scheduler.isShutdown() && this.ckptState.get();
  }

  public void join() throws InterruptedException, ExecutionException {
    this.future.get().get();
  }

  public Boolean call() throws Exception {
    BlockingQueue<BSPSerializableMessage> queue = 
      this.messageDeserializer.messageQueue();
    while(isAlive()) {
      BSPSerializableMessage msg = queue.take();
      String path = msg.checkpointedPath();
      if(null == path || path.toString().isEmpty()) 
        throw new NullPointerException("File dest is not provided.");
      FSDataOutputStream out = this.dfs.create(new Path(path)); 
      msg.messageBundle().write(out);
      try { } finally { try { out.close(); } catch(IOException e) {
        LOG.error("Fail to close hdfs output stream.", e); } 
      } 
    }
    try {  } finally { LOG.info("Stop checkpointing."); this.stop(); }
    return new Boolean(true);
  }
}
