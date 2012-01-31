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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.ipc.GroomProtocol;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.util.BSPNetUtils;
import org.apache.hama.zookeeper.QuorumPeer;
import org.apache.log4j.LogManager;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * A Groom Server (shortly referred to as groom) is a process that performs bsp
 * tasks assigned by BSPMaster. Each groom contacts the BSPMaster, and it takes
 * assigned tasks and reports its status by means of periodical piggybacks with
 * BSPMaster. Each groom is designed to run with HDFS or other distributed
 * storages. Basically, a groom server and a data node should be run on one
 * physical node.
 */
public class GroomServer implements Runnable, GroomProtocol, BSPPeerProtocol,
    Watcher {
  public static final Log LOG = LogFactory.getLog(GroomServer.class);
  static final String SUBDIR = "groomServer";

  private volatile static int REPORT_INTERVAL = 1 * 1000;

  final Configuration conf;

  // Constants
  static enum State {
    NORMAL, COMPUTE, SYNC, BARRIER, STALE, INTERRUPTED, DENIED
  };

  private static ZooKeeper zk = null;

  // Running States and its related things
  volatile boolean initialized = false;
  volatile boolean running = true;
  volatile boolean shuttingDown = false;
  boolean justInited = true;
  GroomServerStatus status = null;

  // Attributes
  String groomServerName;
  String localHostname;
  String groomHostName;
  InetSocketAddress bspMasterAddr;
  private Instructor instructor;

  // Filesystem
  // private LocalDirAllocator localDirAllocator;
  Path systemDirectory = null;
  FileSystem systemFS = null;

  // Job
  private int failures;
  private int maxCurrentTasks;
  Map<TaskAttemptID, TaskInProgress> tasks = new HashMap<TaskAttemptID, TaskInProgress>();
  /** Map from taskId -> TaskInProgress. */
  Map<TaskAttemptID, TaskInProgress> runningTasks = null;
  Map<TaskAttemptID, TaskInProgress> finishedTasks = null;
  Map<TaskAttemptID, Integer> assignedPeerNames = null;
  Map<BSPJobID, RunningJob> runningJobs = null;

  // new nexus between GroomServer and BSPMaster
  // holds/ manage all tasks
  // List<TaskInProgress> tasksList = new
  // CopyOnWriteArrayList<TaskInProgress>();

  private String rpcServer;
  private Server workerServer;
  MasterProtocol masterClient;

  InetSocketAddress taskReportAddress;
  Server taskReportServer = null;

  // private BlockingQueue<GroomServerAction> tasksToCleanup = new
  // LinkedBlockingQueue<GroomServerAction>();

  private class DispatchTasksHandler implements DirectiveHandler {

    public void handle(Directive directive) throws DirectiveException {
      GroomServerAction[] actions = ((DispatchTasksDirective) directive)
          .getActions();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Got Response from BSPMaster with "
            + ((actions != null) ? actions.length : 0) + " actions");
      }

      if (actions != null) {
        assignedPeerNames = new HashMap<TaskAttemptID, Integer>();
        int prevPort = Constants.DEFAULT_PEER_PORT;

        for (GroomServerAction action : actions) {
          if (action instanceof LaunchTaskAction) {
            Task t = ((LaunchTaskAction) action).getTask();

            prevPort = BSPNetUtils.getNextAvailable(prevPort);
            assignedPeerNames.put(t.getTaskID(), prevPort);

            LOG.info("Launch " + actions.length + " tasks.");
            startNewTask((LaunchTaskAction) action);
          } else {

            // TODO Use the cleanup thread
            // tasksToCleanup.put(action);

            LOG.info("Kill " + actions.length + " tasks.");
            KillTaskAction killAction = (KillTaskAction) action;
            if (tasks.containsKey(killAction.getTaskID())) {
              TaskInProgress tip = tasks.get(killAction.getTaskID());
              tip.taskStatus.setRunState(TaskStatus.State.FAILED);
              try {
                tip.killAndCleanup(false);
              } catch (IOException ioe) {
                throw new DirectiveException("Error when killing a "
                    + "TaskInProgress.", ioe);
              }
            }
          }
        }
      }
    }
  }

  private class Instructor extends Thread {
    final BlockingQueue<Directive> buffer = new LinkedBlockingQueue<Directive>();
    final ConcurrentMap<Class<? extends Directive>, DirectiveHandler> handlers = new ConcurrentHashMap<Class<? extends Directive>, DirectiveHandler>();

    public void bind(Class<? extends Directive> instruction,
        DirectiveHandler handler) {
      handlers.putIfAbsent(instruction, handler);
    }

    public void put(Directive directive) {
      try {
        buffer.put(directive);
      } catch (InterruptedException ie) {
        LOG.error("Unable to put directive into queue.", ie);
        Thread.currentThread().interrupt();
      }
    }

    public void run() {
      while (true) {
        try {
          Directive directive = buffer.take();
          handlers.get(directive.getClass()).handle(directive);
        } catch (InterruptedException ie) {
          LOG.error("Unable to retrieve directive from the queue.", ie);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          LOG.error("Fail to execute directive.", e);
        }
      }
    }
  }

  public GroomServer(Configuration conf) throws IOException {
    LOG.info("groom start");
    this.conf = conf;
    bspMasterAddr = BSPMaster.getAddress(conf);

    if (bspMasterAddr == null) {
      System.out.println(BSPMaster.localModeMessage);
      LOG.info(BSPMaster.localModeMessage);
      System.exit(0);
    }

    // FileSystem local = FileSystem.getLocal(conf);
    // this.localDirAllocator = new LocalDirAllocator("bsp.local.dir");

    try {
      zk = new ZooKeeper(QuorumPeer.getZKQuorumServersString(conf),
          conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 1200000), this);
    } catch (IOException e) {
      LOG.error("Exception during reinitialization!", e);
    }
  }

  public synchronized void initialize() throws IOException {
    if (this.conf.get(Constants.PEER_HOST) != null) {
      this.localHostname = conf.get(Constants.PEER_HOST);
    }

    if (localHostname == null) {
      this.localHostname = DNS.getDefaultHost(
          conf.get("bsp.dns.interface", "default"),
          conf.get("bsp.dns.nameserver", "default"));
    }
    // check local disk
    checkLocalDirs(getLocalDirs());
    deleteLocalFiles(SUBDIR);

    // Clear out state tables
    this.tasks.clear();
    this.runningJobs = new TreeMap<BSPJobID, RunningJob>();
    this.runningTasks = new ConcurrentHashMap<TaskAttemptID, TaskInProgress>();
    this.finishedTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.conf.set(Constants.PEER_HOST, localHostname);
    this.conf.set(Constants.GROOM_RPC_HOST, localHostname);
    this.maxCurrentTasks = conf.getInt(Constants.MAX_TASKS_PER_GROOM, 3);

    int rpcPort = -1;
    String rpcAddr = null;
    if (false == this.initialized) {
      rpcAddr = conf.get(Constants.GROOM_RPC_HOST,
          Constants.DEFAULT_GROOM_RPC_HOST);
      rpcPort = conf.getInt(Constants.GROOM_RPC_PORT,
          Constants.DEFAULT_GROOM_RPC_PORT);
      if (-1 == rpcPort || null == rpcAddr)
        throw new IllegalArgumentException("Error rpc address " + rpcAddr
            + " port" + rpcPort);
      this.workerServer = RPC.getServer(this, rpcAddr, rpcPort, conf);
      this.workerServer.start();
      this.rpcServer = rpcAddr + ":" + rpcPort;

      LOG.info("Worker rpc server --> " + rpcServer);
    }

    @SuppressWarnings("deprecation")
    String address = NetUtils.getServerAddress(conf,
        "bsp.groom.report.bindAddress", "bsp.groom.report.port",
        "bsp.groom.report.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();

    // RPC initialization
    // TODO numHandlers should be a ..
    this.taskReportServer = RPC.getServer(this, bindAddress, tmpPort, 10,
        false, this.conf);

    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.conf.set("bsp.groom.report.address", taskReportAddress.getHostName()
        + ":" + taskReportAddress.getPort());
    LOG.info("GroomServer up at: " + this.taskReportAddress);

    this.groomHostName = rpcAddr;
    this.groomServerName = "groomd_" + this.rpcServer.replace(':', '_');
    LOG.info("Starting groom: " + this.rpcServer);

    // establish the communication link to bsp master
    this.masterClient = (MasterProtocol) RPC.waitForProxy(MasterProtocol.class,
        MasterProtocol.versionID, bspMasterAddr, conf);

    // enroll in bsp master
    if (-1 == rpcPort || null == rpcAddr)
      throw new IllegalArgumentException("Error rpc address " + rpcAddr
          + " port" + rpcPort);
    if (!this.masterClient.register(new GroomServerStatus(groomServerName,
        cloneAndResetRunningTaskStatuses(), failures, maxCurrentTasks,
        this.rpcServer, groomHostName))) {
      LOG.error("There is a problem in establishing communication"
          + " link with BSPMaster");
      throw new IOException("There is a problem in establishing"
          + " communication link with BSPMaster.");
    }

    this.instructor = new Instructor();
    this.instructor.bind(DispatchTasksDirective.class,
        new DispatchTasksHandler());
    instructor.start();
    this.running = true;
    this.initialized = true;
  }

  /** Return the port at which the tasktracker bound to */
  public synchronized InetSocketAddress getTaskTrackerReportAddress() {
    return taskReportAddress;
  }

  @Override
  public void dispatch(Directive directive) throws IOException {
    if (!instructor.isAlive())
      throw new IOException();
    instructor.put(directive);
  }

  private static void checkLocalDirs(String[] localDirs)
      throws DiskErrorException {
    boolean writable = false;

    LOG.debug(localDirs);

    if (localDirs != null) {
      for (int i = 0; i < localDirs.length; i++) {
        try {
          LOG.info(localDirs[i]);
          DiskChecker.checkDir(new File(localDirs[i]));
          writable = true;
        } catch (DiskErrorException e) {
          LOG.warn("BSP Processor local " + e.getMessage());
        }
      }
    }

    if (!writable)
      throw new DiskErrorException("all local directories are not writable");
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
    while (running && !shuttingDown) {
      try {

        List<TaskStatus> taskStatuses = new ArrayList<TaskStatus>();
        // Reports to a BSPMaster
        for (Map.Entry<TaskAttemptID, TaskInProgress> e : runningTasks
            .entrySet()) {
          TaskInProgress tip = e.getValue();
          TaskStatus taskStatus = tip.getStatus();
          taskStatuses.add(taskStatus);
        }

        doReport(taskStatuses);
        Thread.sleep(REPORT_INTERVAL);
      } catch (InterruptedException ie) {
      }

      try {
        if (justInited) {
          String dir = masterClient.getSystemDir();
          if (dir == null) {
            LOG.error("Fail to get system directory.");
            throw new IOException("Fail to get system directory.");
          }
          systemDirectory = new Path(dir);
          systemFS = systemDirectory.getFileSystem(conf);
        }
        justInited = false;
      } catch (DiskErrorException de) {
        String msg = "Exiting groom server for disk error:\n"
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
      Thread.sleep(REPORT_INTERVAL);
    }
    return State.NORMAL;
  }

  private void startNewTask(LaunchTaskAction action) {
    Task t = action.getTask();
    BSPJob jobConf = null;
    try {
      jobConf = new BSPJob(t.getJobID(), t.getJobFile());
    } catch (IOException e1) {
      LOG.error(e1);
    }

    TaskInProgress tip = new TaskInProgress(t, jobConf, this.groomServerName);

    synchronized (this) {
      tasks.put(t.getTaskID(), tip);
      runningTasks.put(t.getTaskID(), tip);
    }

    try {
      localizeJob(tip);
    } catch (Throwable e) {
      String msg = ("Error initializing " + tip.getTask().getTaskID() + ":\n" + StringUtils
          .stringifyException(e));
      LOG.warn(msg);

      try {
        tip.killAndCleanup(true);
      } catch (IOException ie2) {
        LOG.info("Error cleaning up " + tip.getTask().getTaskID() + ":\n"
            + StringUtils.stringifyException(ie2));
      }
    }
  }

  /**
   * Update and report refresh status back to BSPMaster.
   */
  public void doReport(List<TaskStatus> taskStatuses) {
    GroomServerStatus groomStatus = new GroomServerStatus(groomServerName,
        updateTaskStatuses(taskStatuses), failures, maxCurrentTasks, rpcServer,
        groomHostName);
    try {
      boolean ret = masterClient.report(new ReportGroomStatusDirective(
          groomStatus));
      if (!ret) {
        LOG.warn("Fail to renew BSPMaster's GroomServerStatus. "
            + " groom name: " + groomStatus.getGroomName() + " rpc server:"
            + rpcServer);
      }
    } catch (IOException ioe) {
      LOG.error("Fail to communicate with BSPMaster for reporting.", ioe);
    }
  }

  public List<TaskStatus> updateTaskStatuses(List<TaskStatus> taskStatuses) {
    List<TaskStatus> tlist = new ArrayList<TaskStatus>();

    for (TaskStatus taskStatus : taskStatuses) {
      if (taskStatus.getRunState() == TaskStatus.State.SUCCEEDED
          || taskStatus.getRunState() == TaskStatus.State.FAILED) {
        synchronized (finishedTasks) {
          TaskInProgress tip = runningTasks.remove(taskStatus.getTaskId());
          tlist.add((TaskStatus) taskStatus.clone());
          finishedTasks.put(taskStatus.getTaskId(), tip);
        }
      } else if (taskStatus.getRunState() == TaskStatus.State.RUNNING) {
        tlist.add((TaskStatus) taskStatus.clone());
      }
    }
    return tlist;
  }

  private void localizeJob(TaskInProgress tip) throws IOException {
    Task task = tip.getTask();
    conf.addResource(task.getJobFile());
    BSPJob defaultJobConf = new BSPJob((HamaConfiguration) conf);
    Path localJobFile = defaultJobConf.getLocalPath(SUBDIR + "/"
        + task.getTaskID() + "/" + "job.xml");

    RunningJob rjob = addTaskToJob(task.getJobID(), localJobFile, tip);
    BSPJob jobConf = null;

    synchronized (rjob) {
      if (!rjob.localized) {

        FileSystem localFs = FileSystem.getLocal(conf);
        Path jobDir = localJobFile.getParent();
        if (localFs.exists(jobDir)) {
          localFs.delete(jobDir, true);
          boolean b = localFs.mkdirs(jobDir);
          if (!b)
            throw new IOException("Not able to create job directory "
                + jobDir.toString());
        }

        Path localJarFile = defaultJobConf.getLocalPath(SUBDIR + "/"
            + task.getTaskID() + "/" + "job.jar");
        systemFS.copyToLocalFile(new Path(task.getJobFile()), localJobFile);

        HamaConfiguration conf = new HamaConfiguration();
        conf.addResource(localJobFile);
        jobConf = new BSPJob(conf, task.getJobID().toString());

        Path jarFile = null;
        if (jobConf.getJar() != null) {
          jarFile = new Path(jobConf.getJar());
        } else {
          LOG.warn("No jar file for job " + task.getJobID()
              + " has been defined!");
        }
        jobConf.setJar(localJarFile.toString());

        if (jarFile != null) {
          systemFS.copyToLocalFile(jarFile, localJarFile);

          // also unjar the job.jar files in workdir
          File workDir = new File(
              new File(localJobFile.toString()).getParent(), "work");
          if (!workDir.mkdirs()) {
            if (!workDir.isDirectory()) {
              throw new IOException("Mkdirs failed to create "
                  + workDir.toString());
            }
          }
          RunJar.unJar(new File(localJarFile.toString()), workDir);
        }
        rjob.localized = true;
      } else {
        HamaConfiguration conf = new HamaConfiguration();
        conf.addResource(rjob.getJobFile());
        jobConf = new BSPJob(conf, rjob.getJobId().toString());
      }
    }

    launchTaskForJob(tip, jobConf);
  }

  private void launchTaskForJob(TaskInProgress tip, BSPJob jobConf) {
    try {
      tip.setJobConf(jobConf);
      tip.launchTask();
    } catch (Throwable ie) {
      tip.taskStatus.setRunState(TaskStatus.State.FAILED);
      String error = StringUtils.stringifyException(ie);
      LOG.info(error);
    }
  }

  private RunningJob addTaskToJob(BSPJobID jobId, Path localJobFile,
      TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rJob = null;
      if (!runningJobs.containsKey(jobId)) {
        rJob = new RunningJob(jobId, localJobFile);
        rJob.localized = false;
        rJob.tasks = new HashSet<TaskInProgress>();
        rJob.jobFile = localJobFile;
        runningJobs.put(jobId, rJob);
      } else {
        rJob = runningJobs.get(jobId);
      }
      rJob.tasks.add(tip);
      return rJob;
    }
  }

  /**
   * The datastructure for initializing a job
   */
  static class RunningJob {
    private BSPJobID jobid;
    private Path jobFile;
    // keep this for later use
    Set<TaskInProgress> tasks;
    boolean localized;
    boolean keepJobFiles;

    RunningJob(BSPJobID jobid, Path jobFile) {
      this.jobid = jobid;
      localized = false;
      tasks = new HashSet<TaskInProgress>();
      this.jobFile = jobFile;
      keepJobFiles = false;
    }

    Path getJobFile() {
      return jobFile;
    }

    BSPJobID getJobId() {
      return jobid;
    }
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
                LOG.info("Lost connection to BSP Master [" + bspMasterAddr
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
    try {
      zk.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    this.running = false;
    this.initialized = false;
    cleanupStorage();
    this.workerServer.stop();
    RPC.stopProxy(masterClient);
    if (taskReportServer != null) {
      taskReportServer.stop();
      taskReportServer = null;
    }
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
  // its TaskStatus, and the BSPTaskRunner.
  // /////////////////////////////////////////////////////
  class TaskInProgress {
    Task task;
    BSPJob jobConf;
    BSPJob localJobConf;
    BSPTaskRunner runner;
    volatile boolean done = false;
    volatile boolean wasKilled = false;
    private TaskStatus taskStatus;

    public TaskInProgress(Task task, BSPJob jobConf, String groomServer) {
      this.task = task;
      this.jobConf = jobConf;
      this.localJobConf = null;
      this.taskStatus = new TaskStatus(task.getJobID(), task.getTaskID(), 0,
          TaskStatus.State.UNASSIGNED, "init", groomServer,
          TaskStatus.Phase.STARTING, task.getCounters());
    }

    private void localizeTask(Task task) throws IOException {
      Path localJobFile = this.jobConf.getLocalPath(SUBDIR + "/"
          + task.getTaskID() + "/job.xml");
      Path localJarFile = this.jobConf.getLocalPath(SUBDIR + "/"
          + task.getTaskID() + "/job.jar");

      String jobFile = task.getJobFile();
      systemFS.copyToLocalFile(new Path(jobFile), localJobFile);
      task.setJobFile(localJobFile.toString());

      localJobConf = new BSPJob(task.getJobID(), localJobFile.toString());
      localJobConf.set("bsp.task.id", task.getTaskID().toString());
      String jarFile = localJobConf.getJar();

      if (jarFile != null) {
        systemFS.copyToLocalFile(new Path(jarFile), localJarFile);
        localJobConf.setJar(localJarFile.toString());
      }

      task.setConf(localJobConf);
    }

    public synchronized void setJobConf(BSPJob jobConf) {
      this.jobConf = jobConf;
    }

    public synchronized BSPJob getJobConf() {
      return localJobConf;
    }

    public void launchTask() throws IOException {
      localizeTask(task);
      taskStatus.setRunState(TaskStatus.State.RUNNING);
      this.runner = task.createRunner(GroomServer.this);
      this.runner.start();
      LOG.info("Task '" + task.getTaskID().toString() + "' has started.");
    }

    /**
     * Something went wrong and the task must be killed.
     */
    public synchronized void killAndCleanup(boolean wasFailure)
        throws IOException {
      if (wasFailure) {
        failures += 1;
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      }

      // runner could be null if task-cleanup attempt is not localized yet
      if (runner != null) {
        runner.killBsp();
      }
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

    public void reportProgress(TaskStatus taskStatus) {
      // LOG.info(task.getTaskID() + " " + taskStatus.getProgress() + "% "
      // + taskStatus.getStateString());

      if (this.done) {
        LOG.info(task.getTaskID()
            + " Ignoring status-update since "
            + ((this.done) ? "task is 'done'" : ("runState: " + this.taskStatus
                .getRunState())));
        return;
      }

      this.taskStatus.statusUpdate(taskStatus);
    }

    public void reportDone() {
      if (this.taskStatus.getRunState() != TaskStatus.State.FAILED) {
        this.taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
      }

      this.taskStatus.setFinishTime(System.currentTimeMillis());
      this.done = true;
      this.runner.killBsp();
      LOG.info("Task " + task.getTaskID() + " is done.");
    }

    public void jobHasFinished(boolean wasFailure) throws IOException {
      // Kill the task if it is still running
      synchronized (this) {
        if (getRunState() == TaskStatus.State.RUNNING
            || getRunState() == TaskStatus.State.UNASSIGNED
            || getRunState() == TaskStatus.State.COMMIT_PENDING) {
          killAndCleanup(wasFailure);
        }
      }
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

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(GroomProtocol.class.getName())) {
      return GroomProtocol.versionID;
    } else if (protocol.equals(BSPPeerProtocol.class.getName())) {
      return BSPPeerProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to GroomServer: " + protocol);
    }
  }

  /**
   * Remove the tip and update all relevant state.
   * 
   * @param tip {@link TaskInProgress} to be removed.
   * @param wasFailure did the task fail or was it killed?
   */
  private void purgeTask(TaskInProgress tip, boolean wasFailure)
      throws IOException {
    if (tip != null) {
      LOG.info("About to purge task: " + tip.getTask().getTaskID());

      // Remove the task from running jobs,
      // removing the job if it's the last task
      removeTaskFromJob(tip.getTask().getJobID(), tip);
      tip.jobHasFinished(wasFailure);
    }
  }

  private void removeTaskFromJob(BSPJobID jobId, TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rjob = runningJobs.get(jobId);
      if (rjob == null) {
        LOG.warn("Unknown job " + jobId + " being deleted.");
      } else {
        synchronized (rjob) {
          rjob.tasks.remove(tip);
        }
      }
    }
  }

  /**
   * The main() for BSPPeer child processes.
   */
  public static final class BSPPeerChild {

    public static void main(String[] args) throws Throwable {
      if (LOG.isDebugEnabled())
        LOG.debug("BSPPeerChild starting");

      final HamaConfiguration defaultConf = new HamaConfiguration();
      // report address
      String host = args[0];
      int port = Integer.parseInt(args[1]);
      InetSocketAddress address = new InetSocketAddress(host, port);
      TaskAttemptID taskid = TaskAttemptID.forName(args[2]);

      // //////////////////
      BSPPeerProtocol umbilical = (BSPPeerProtocol) RPC.getProxy(
          BSPPeerProtocol.class, BSPPeerProtocol.versionID, address,
          defaultConf);

      final BSPTask task = (BSPTask) umbilical.getTask(taskid);
      int peerPort = umbilical.getAssignedPortNum(taskid);

      defaultConf.addResource(new Path(task.getJobFile()));
      BSPJob job = new BSPJob(task.getJobID(), task.getJobFile());

      defaultConf.set(Constants.PEER_HOST, args[3]);
      if (null != args && 5 == args.length) {
        defaultConf.setInt("bsp.checkpoint.port", Integer.parseInt(args[4]));
      }
      defaultConf.setInt(Constants.PEER_PORT, peerPort);

      try {
        // use job-specified working directory
        FileSystem.get(job.getConf()).setWorkingDirectory(
            job.getWorkingDirectory());

        // instantiate and init our peer
        @SuppressWarnings("rawtypes")
        final BSPPeerImpl<?, ?, ?, ?> bspPeer = new BSPPeerImpl(job,
            defaultConf, taskid, umbilical, task.partition, task.splitClass,
            task.split, task.getCounters());

        task.run(job, bspPeer, umbilical); // run the task

      } catch (FSError e) {
        LOG.fatal("FSError from child", e);
        umbilical.fsError(taskid, e.getMessage());
      } catch (SyncException e) {
        LOG.fatal("SyncError from child", e);
        umbilical.fatalError(taskid, e.toString());

        // Report back any failures, for diagnostic purposes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(baos));
      } catch (Throwable throwable) {
        LOG.warn("Error running child", throwable);
        // Report back any failures, for diagnostic purposes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        throwable.printStackTrace(new PrintStream(baos));
      } finally {
        RPC.stopProxy(umbilical);
        // Shutting down log4j of the child-vm...
        // This assumes that on return from Task.run()
        // there is no more logging done.
        LogManager.shutdown();
      }
    }
  }

  @Override
  public Task getTask(TaskAttemptID taskid) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      return tip.getTask();
    } else {
      return null;
    }
  }

  @Override
  public boolean ping(TaskAttemptID taskid) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * A child task had a fatal error. Kill the task.
   */
  @Override
  public void fatalError(TaskAttemptID taskId, String message)
      throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed : " + message);
    TaskInProgress tip = runningTasks.get(taskId);

    purgeTask(tip, true);
  }

  @Override
  public void fsError(TaskAttemptID taskId, String message) throws IOException {
    LOG.fatal("Task: " + taskId + " - Killed due to FSError: " + message);
    // TODO

  }

  @Override
  public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
      throws IOException, InterruptedException {
    TaskInProgress tip = tasks.get(taskId);
    if (tip != null) {
      tip.reportProgress(taskStatus);
      return true;
    } else {
      LOG.warn("Progress from unknown child task: " + taskId);
      return false;
    }
  }

  @Override
  public void done(TaskAttemptID taskid) throws IOException {
    TaskInProgress tip = tasks.get(taskid);
    if (tip != null) {
      tip.reportDone();
    } else {
      LOG.warn("Unknown child task done: " + taskid + ". Ignored.");
    }
  }

  @Override
  public int getAssignedPortNum(TaskAttemptID taskid) {
    return assignedPeerNames.get(taskid);
  }

  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub

  }

}
