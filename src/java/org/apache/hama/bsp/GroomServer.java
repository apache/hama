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
package org.apache.hama.bsp;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.InterTrackerProtocol;

public class GroomServer implements Runnable {
  public static final Log LOG = LogFactory.getLog(GroomServer.class);
  private static BSPPeer bspPeer;
  private Task task;

  Configuration conf;

  // Constants
  static enum State {
    NORMAL, COMPUTE, SYNC, BARRIER, STALE, INTERRUPTED, DENIED
  };

  // Running States and its related things
  volatile boolean running = true;
  volatile boolean shuttingDown = false;
  boolean justStarted = true;
  boolean justInited = true;
  GroomServerStatus status = null;
  short heartbeatResponseId = -1;
  private volatile int heartbeatInterval = 3 * 1000;

  // Attributes
  String groomServerName;
  String localHostname;
  InetSocketAddress bspMasterAddr;
  InterTrackerProtocol jobClient;

  // Filesystem
  // private LocalDirAllocator localDirAllocator;
  Path systemDirectory = null;
  FileSystem systemFS = null;

  // Job
  boolean acceptNewTasks = true;
  private int failures;
  private int maxCurrentTasks;
  Map<TaskAttemptID, TaskInProgress> tasks = new HashMap<TaskAttemptID, TaskInProgress>();
  /** Map from taskId -> TaskInProgress. */
  Map<TaskAttemptID, TaskInProgress> runningTasks = null;
  Map<BSPJobID, RunningJob> runningJobs = null;

  private BlockingQueue<GroomServerAction> tasksToCleanup = new LinkedBlockingQueue<GroomServerAction>();

  public GroomServer(Configuration conf) throws IOException {
    LOG.info("groom start");
    this.conf = conf;
    String mode = conf.get("bsp.master.address");
    if (!mode.equals("local")) {
      bspMasterAddr = BSPMaster.getAddress(conf);
    }
    bspPeer = new BSPPeer(conf);

    // FileSystem local = FileSystem.getLocal(conf);
    // this.localDirAllocator = new LocalDirAllocator("bsp.local.dir");
  }

  public synchronized void initialize() throws IOException {
    if (this.conf.get("slave.host.name") != null) {
      this.localHostname = conf.get("slave.host.name");
    }

    if (localHostname == null) {
      this.localHostname = DNS.getDefaultHost(conf.get("bsp.dns.interface",
          "default"), conf.get("bsp.dns.nameserver", "default"));
    }

    // check local disk
    checkLocalDirs(conf.getStrings("bsp.local.dir"));
    deleteLocalFiles("groomserver");

    // Clear out state tables
    this.tasks.clear();
    this.runningJobs = new TreeMap<BSPJobID, RunningJob>();
    this.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.acceptNewTasks = true;

    this.groomServerName = "groomd_" + localHostname;
    LOG.info("Starting tracker " + this.groomServerName);

    DistributedCache.purgeCache(this.conf);

    this.jobClient = (InterTrackerProtocol) RPC.waitForProxy(
        InterTrackerProtocol.class, InterTrackerProtocol.versionID,
        bspMasterAddr, conf);
    this.running = true;
    // this.bspPeer = new BSPPeer(this.conf);
  }

  private static void checkLocalDirs(String[] localDirs)
      throws DiskErrorException {
    boolean writable = false;

    LOG.info(localDirs);

    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          LOG.info(localDirs[i]);
          DiskChecker.checkDir(new File(localDirs[i]));
          writable = true;
        } catch (DiskErrorException e) {
          LOG.warn("Graph Processor local " + e.getMessage());
        }
      }
    }

    // if (!writable)
    // throw new DiskErrorException("all local directories are not writable");
  }

  public String[] getLocalDirs() {
    return conf.getStrings("bsp.local.dir");
  }

  public void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getLocal(this.conf).delete(new Path(localDirs[i]), true);
    }
  }

  public void deleteLocalFiles(String subdir) throws IOException {
    try {
      String[] localDirs = getLocalDirs();
      for (int i = 0; i < localDirs.length; i++) {
        FileSystem.getLocal(this.conf).delete(new Path(localDirs[i], subdir),
            true);
      }
    } catch (NullPointerException e) {
      LOG.info(e);
    }
  }

  public void cleanupStorage() throws IOException {
    deleteLocalFiles();
  }

  private void startCleanupThreads() throws IOException {

  }

  public State offerService() throws Exception {
    long lastHeartbeat = 0;

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();

        long waitTime = heartbeatInterval - (now - lastHeartbeat);
        if (waitTime > 0) {
          // sleeps for the wait time
          Thread.sleep(waitTime);
        }

        if (justInited) {
          String dir = jobClient.getSystemDir();
          if (dir == null) {
            throw new IOException("Failed to get system directory");
          }
          systemDirectory = new Path(dir);
          systemFS = systemDirectory.getFileSystem(conf);
        }

        // Send the heartbeat and process the bspmaster's directives
        HeartbeatResponse heartbeatResponse = transmitHeartBeat(now);

        GroomServerAction[] actions = heartbeatResponse.getActions();
        LOG.info("Got heartbeatResponse from BSPMaster with responseId: "
            + heartbeatResponse.getResponseId() + " and "
            + ((actions != null) ? actions.length : 0) + " actions");

        if (actions != null) {
          for (GroomServerAction action : actions) {
            if (action instanceof LaunchTaskAction) {
              startNewTask((LaunchTaskAction) action);
            } else {
              tasksToCleanup.put(action);
            }
          }
        }

        //
        // The heartbeat got through successfully!
        //
        heartbeatResponseId = heartbeatResponse.getResponseId();

        // Note the time when the heartbeat returned, use this to decide when to
        // send the
        // next heartbeat
        lastHeartbeat = System.currentTimeMillis();

        justStarted = false;
        justInited = false;
      } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return State.INTERRUPTED;
      } catch (DiskErrorException de) {
        String msg = "Exiting task tracker for disk error:\n"
            + StringUtils.stringifyException(de);
        LOG.error(msg);

        return State.STALE;
      } catch (RemoteException re) {
        return State.DENIED;
      } catch (Exception except) {
        String msg = "Caught exception: "
            + StringUtils.stringifyException(except);
        LOG.error(msg);
      }
    }

    return State.NORMAL;
  }

  private void startNewTask(LaunchTaskAction action) {
    // TODO Auto-generated method stub
    task = action.getTask();
    this.launchTask();
    // LOG.info("GroomServer: " + t.getJobID() + ", " + t.getJobFile() + ", " +
    // t.getId() + ", " + t.getPartition());
    // LOG.info(t.runner);
    // t.runner.start();
    // TODO: execute task

  }

  private HeartbeatResponse transmitHeartBeat(long now) throws IOException {
    // 
    // Check if the last heartbeat got through...
    // if so then build the heartbeat information for the BSPMaster;
    // else resend the previous status information.
    //
    if (status == null) {
      synchronized (this) {
        status = new GroomServerStatus(groomServerName, localHostname,
            cloneAndResetRunningTaskStatuses(), failures, maxCurrentTasks);
      }
    } else {
      LOG.info("Resending 'status' to '" + bspMasterAddr.getHostName()
          + "' with reponseId '" + heartbeatResponseId + "'");
    }

    // TODO - Later, acceptNewTask is to be set by the status of groom server.
    HeartbeatResponse heartbeatResponse = jobClient.heartbeat(status,
        justStarted, justInited, acceptNewTasks, heartbeatResponseId);
    return heartbeatResponse;
  }

  private synchronized List<TaskStatus> cloneAndResetRunningTaskStatuses() {
    List<TaskStatus> result = new ArrayList<TaskStatus>(runningTasks.size());
    for (TaskInProgress tip : runningTasks.values()) {
      TaskStatus status = tip.getStatus();
      result.add((TaskStatus) status.clone());
    }
    return result;
  }

  public void run() {
    try {
      initialize();
      startCleanupThreads();
      boolean denied = false;
      while (running && !shuttingDown && !denied) {

        boolean staleState = false;
        try {
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception e) {
              if (!shuttingDown) {
                LOG.info("Lost connection to GraphProcessor [" + bspMasterAddr
                    + "].  Retrying...", e);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
          }
        } finally {
          // close();
        }

        if (shuttingDown) {
          return;
        }
        LOG.warn("Reinitializing local state");
        initialize();
      }
    } catch (IOException ioe) {
      LOG.error("Got fatal exception while reinitializing GroomServer: "
          + StringUtils.stringifyException(ioe));
      return;
    }
  }

  public synchronized void shutdown() throws IOException {
    shuttingDown = true;
    close();
  }

  public synchronized void close() throws IOException {
    this.running = false;
    bspPeer.close();
    cleanupStorage();

    // shutdown RPC connections
    RPC.stopProxy(jobClient);
  }

  public static Thread startGroomServer(final GroomServer hrs) {
    return startGroomServer(hrs, "regionserver" + hrs.groomServerName);
  }

  public static Thread startGroomServer(final GroomServer hrs, final String name) {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    return t;
  }

  // /////////////////////////////////////////////////////
  // TaskInProgress maintains all the info for a Task that
  // lives at this GroomServer. It maintains the Task object,
  // its TaskStatus, and the TaskRunner.
  // /////////////////////////////////////////////////////
  class TaskInProgress {
    Task task;
    volatile boolean done = false;
    volatile boolean wasKilled = false;
    private TaskStatus taskStatus;

    public TaskInProgress(Task task, BSPJobContext job) {
      this.task = task;
    }

    /**
     */
    public Task getTask() {
      return task;
    }

    /**
     */
    public synchronized TaskStatus getStatus() {
      return taskStatus;
    }

    /**
     */
    public TaskStatus.State getRunState() {
      return taskStatus.getRunState();
    }

    public boolean wasKilled() {
      return wasKilled;
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof TaskInProgress)
          && task.getTaskID().equals(
              ((TaskInProgress) obj).getTask().getTaskID());
    }

    @Override
    public int hashCode() {
      return task.getTaskID().hashCode();
    }
  }

  public boolean isRunning() {
    return running;
  }

  public static GroomServer constructGroomServer(
      Class<? extends GroomServer> groomServerClass, final Configuration conf2) {
    try {
      Constructor<? extends GroomServer> c = groomServerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf2);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Master: "
          + groomServerClass.toString(), e);
    }
  }

  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(GroomServer.class, args, LOG);
    if (args.length != 0) {
      System.out.println("usage: GroomServer");
      System.exit(-1);
    }

    try {
      Configuration conf = new HamaConfiguration();

      // conf.set(Constants.PEER_PORT, String.valueOf(30000));
      GroomServer groom = GroomServer.constructGroomServer(GroomServer.class,
          conf);
      startGroomServer(groom);
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  public void assignTask(Task task) {
    this.task = task;
  }

  public void launchTask() {
    // until job is completed, don't accept new task
    acceptNewTasks = false;

    // TODO: check the job file and task localizing and execute them.
    LOG.info("Launch a task");
    
    LOG.info("JobFile: " + task.getJobFile());
    LOG.info("Job Partition: " + task.getPartition());
    
    /*
    try {
      BSPJob jobConf = new BSPJob();
      jobConf.addResource(new Path(task.getJobFile().replace("file:", "")));
      LOG.info("Job File>>>>> " + task.getJobFile().replace("file:", ""));
      BSP bsp = (BSP) ReflectionUtils.newInstance(jobConf.getClass("bsp.work.class", BSP.class), conf);
      bsp.setPeer(bspPeer);
      bsp.start();
    } catch (Exception e) {
      System.exit(-1);
    }
    */
  }

  public String getServerName() {
    return bspPeer.getServerName();
  }
}
