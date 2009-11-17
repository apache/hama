/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hama;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.graph.JobID;
import org.apache.hama.graph.JobStatus;
import org.apache.hama.ipc.HeartbeatResponse;
import org.apache.hama.ipc.InterTrackerProtocol;
import org.apache.hama.ipc.JobSubmissionProtocol;

public class HamaMaster implements JobSubmissionProtocol, InterTrackerProtocol {
  static{
    Configuration.addDefaultResource("groomserver-default.xml");
  }
  
  public static final Log LOG = LogFactory.getLog(HamaMaster.class);
  
  private HamaConfiguration conf;  
  public static enum State { INITIALIZING, RUNNING }
  State state = State.INITIALIZING;
  
  String masterIdentifier;
  
  private Server interTrackerServer;  
  
  FileSystem fs = null;
  Path systemDir = null;
  
  // system directories are world-wide readable and owner readable
  final static FsPermission SYSTEM_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0733); // rwx-wx-wx

  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0700); // rwx------
  
  private static final int FS_ACCESS_RETRY_PERIOD = 10000;
  
  private int nextJobId = 1;
  
  public HamaMaster(HamaConfiguration conf, String identifier) throws IOException, InterruptedException {
    this.conf = conf;
    
    this.masterIdentifier = identifier;
    
    InetSocketAddress addr = getAddress(conf);    
    this.interTrackerServer = RPC.getServer(this, addr.getHostName(), addr.getPort(), conf);
    
    while (!Thread.currentThread().isInterrupted()) {
      try {
        if (fs == null) {
          fs = FileSystem.get(conf);
        }
        // clean up the system dir, which will only work if hdfs is out of 
        // safe mode
        if(systemDir == null) {
          systemDir = new Path(getSystemDir());    
        }

        LOG.info("Cleaning up the system directory");
        fs.delete(systemDir, true);
        if (FileSystem.mkdirs(fs, systemDir, 
            new FsPermission(SYSTEM_DIR_PERMISSION))) {
          break;
        }
        LOG.error("Mkdirs failed to create " + systemDir);

      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on mapred.system.dir (" + systemDir 
            + ") because of permissions.");
        LOG.warn("Manually delete the mapred.system.dir (" + systemDir 
            + ") and then start the JobTracker.");
        LOG.warn("Bailing out ... ");
        throw ace;
      } catch (IOException ie) {
        LOG.info("problem cleaning system directory: " + systemDir, ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }
    
    // deleteLocalFiles(SUBDIR);
  }
  
  public static HamaMaster startMaster(HamaConfiguration conf) throws IOException,
  InterruptedException {
    return startTracker(conf, generateNewIdentifier());
  }
  
  public static HamaMaster startTracker(HamaConfiguration conf, String identifier) 
  throws IOException, InterruptedException {
    
    HamaMaster result = null;
    result = new HamaMaster(conf, identifier);
    
    return result;
  }
  
  public static InetSocketAddress getAddress(Configuration conf) {
    String hamaMasterStr = conf.get("hama.master.address", "localhost:40000");
    return NetUtils.createSocketAddr(hamaMasterStr);
  }
  
  public int getPort() {
    return this.conf.getInt("hama.master.port", 0);
  }

  public HamaConfiguration getConfiguration() {
    return this.conf;
  }
  
  private static SimpleDateFormat getDateFormat() {
    return new SimpleDateFormat("yyyyMMddHHmm");
  }

  private static String generateNewIdentifier() {
    return getDateFormat().format(new Date());
  }
  
  public void offerService() throws InterruptedException, IOException {
    this.interTrackerServer.start();
    
    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");
    
    this.interTrackerServer.join();
    LOG.info("Stopped interTrackerServer");
  }
  
  
  public static void main(String [] args) {
    StringUtils.startupShutdownMessage(HamaMaster.class, args, LOG);
    if (args.length != 1) {
      System.out.println("usage: HamaMaster");
      System.exit(-1);
    }
      
    try {
      HamaConfiguration conf = new HamaConfiguration();
      HamaMaster master = startMaster(conf);
      master.offerService();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {    
    if (protocol.equals(InterTrackerProtocol.class.getName())) {
      return InterTrackerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())){
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to job tracker: " + protocol);
    }
  }

  @Override
  public HeartbeatResponse heartbeat(short responseId) {
    LOG.debug(">>> return the heartbeat message.");
    return new HeartbeatResponse((short)1);
  }

  @Override
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("mapred.system.dir", "/tmp/hadoop/mapred/system"));  
    return fs.makeQualified(sysDir).toString();
  }

  @Override
  public JobID getNewJobId() throws IOException {
    return new JobID(this.masterIdentifier, nextJobId++);    
  }

  @Override
  public JobStatus submitJob(JobID jobName) throws IOException {
    
    return null;
  }
}
