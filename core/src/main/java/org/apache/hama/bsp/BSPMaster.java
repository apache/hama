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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.MasterSyncClient;
import org.apache.hama.bsp.sync.ZKSyncBSPMasterClient;
import org.apache.hama.http.HttpServer;
import org.apache.hama.ipc.GroomProtocol;
import org.apache.hama.ipc.HamaRPCProtocolVersion;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.monitor.fd.FDProvider;
import org.apache.hama.monitor.fd.Supervisor;
import org.apache.hama.monitor.fd.UDPSupervisor;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * BSPMaster is responsible to control all the groom servers and to manage bsp
 * jobs. It has the following responsibilities:
 * <ol>
 * <li> <b>Job submission</b>. BSPMaster is responsible for accepting new job
 * requests and assigning the job to scheduler for scheduling BSP Tasks defined
 * for the job.
 * <li> <b>GroomServer monitoring</b> BSPMaster keeps track of all the groom 
 * servers in the cluster. It is responsible for adding new grooms to the 
 * cluster and keeping a tab on all the grooms and could blacklist a groom if 
 * it get fails the availability requirement.
 * <li> BSPMaster keeps track of all the task status for each job and handles
 * the failure of job as requested by the jobs.  
 * </ol>
 */
public class BSPMaster implements JobSubmissionProtocol, MasterProtocol,
    GroomServerManager, Watcher, MonitorManager {

  public static final Log LOG = LogFactory.getLog(BSPMaster.class);
  public static final String localModeMessage = "Local mode detected, no launch of the daemon needed.";
  private static final int FS_ACCESS_RETRY_PERIOD = 10000;

  private HamaConfiguration conf;
  MasterSyncClient syncClient = null;

  /**
   * Constants for BSPMaster's status.
   */
  public static enum State {
    INITIALIZING, RUNNING
  }

  static long JOBINIT_SLEEP_INTERVAL = 2000;

  // States
  final AtomicReference<State> state = new AtomicReference<State>(
      State.INITIALIZING);

  // Attributes
  String masterIdentifier;
  // private Server interServer;
  private Server masterServer;

  // host and port
  private String host;
  private int port;

  // startTime
  private long startTime;

  // HTTP server
  private HttpServer infoServer;
  private int infoPort;

  // Filesystem
  static final String SUBDIR = "bspMaster";
  FileSystem fs = null;
  Path systemDir = null;
  // system directories are world-wide readable and owner readable
  final static FsPermission SYSTEM_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0733); // rwx-wx-wx
  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION = FsPermission
      .createImmutable((short) 0700); // rwx------

  // Jobs' Meta Data
  private Integer nextJobId = Integer.valueOf(1);
  private int totalSubmissions = 0; // how many jobs has been submitted by clients
  private int totalTasks = 0; // currnetly running tasks
  private int totalTaskCapacity; // max tasks that groom server can run

  private Map<BSPJobID, JobInProgress> jobs = new TreeMap<BSPJobID, JobInProgress>();
  private TaskScheduler taskScheduler;

  // Gorom Server Manager attributes
  // GroomServers cache
  protected ConcurrentMap<GroomServerStatus, GroomProtocol> groomServers = new ConcurrentHashMap<GroomServerStatus, GroomProtocol>();

  private final List<GroomServerStatus> blackList = new CopyOnWriteArrayList<GroomServerStatus>();

  private Instructor instructor;

  private final List<JobInProgressListener> jobInProgressListeners = new CopyOnWriteArrayList<JobInProgressListener>();

  private final AtomicReference<Supervisor> supervisor = new AtomicReference<Supervisor>();

  /**
   * ReportGroomStatusHandler keeps track of the status reported by each 
   * Groomservers on the task they are executing currently. Based on the 
   * status reported, it is responsible for issuing task recovery requests, 
   * updating the job progress and other book keeping on currently running
   * jobs. 
   */
  private class ReportGroomStatusHandler implements DirectiveHandler {

    @Override
    public void handle(Directive directive) throws DirectiveException {
      // update GroomServerStatus held in the groomServers cache.
      GroomServerStatus groomStatus = ((ReportGroomStatusDirective) directive)
          .getStatus();
      // groomServers cache contains groom server status reported back
      if (groomServers.containsKey(groomStatus)) {
        GroomServerStatus tmpStatus = null;
        for (GroomServerStatus old : groomServers.keySet()) {
          if (old.equals(groomStatus)) {
            totalTasks -= old.countTasks();
            tmpStatus = groomStatus;
            updateGroomServersKey(old, tmpStatus);
            break;
          }
        }

        if (null != tmpStatus) {
          totalTasks += tmpStatus.countTasks();

          List<TaskStatus> tlist = tmpStatus.getTaskReports();
          for (TaskStatus ts : tlist) {
            JobInProgress jip = taskScheduler.findJobById(ts.getJobId());
            TaskInProgress tip = jip.findTaskInProgress(ts.getTaskId()
                .getTaskID());

            if (ts.getRunState() == TaskStatus.State.SUCCEEDED) {
              jip.completedTask(tip, ts);
              // increment counters only if successful
              jip.getCounters().incrAllCounters(ts.getCounters());
            } else if (ts.getRunState() == TaskStatus.State.RUNNING) {
              jip.getStatus().setProgress(ts.getSuperstepCount());
              jip.getStatus().setSuperstepCount(ts.getSuperstepCount());
            } else if (ts.getRunState() == TaskStatus.State.FAILED) {
              if(jip.handleFailure(tip)){
                recoverTask(jip);
              }
              else {
                jip.status.setRunState(JobStatus.FAILED);
                jip.failedTask(tip, ts);
              }
            }
            if (jip.getStatus().getRunState() == JobStatus.SUCCEEDED) {
              for (JobInProgressListener listener : jobInProgressListeners) {
                try {
                  listener.jobRemoved(jip);
                } catch (IOException ioe) {
                  LOG.error("Fail to alter scheduler a job is moved.", ioe);
                }
              }
            } else if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
              jip.getStatus().setProgress(ts.getSuperstepCount());
              jip.getStatus().setSuperstepCount(ts.getSuperstepCount());
            } else if (jip.getStatus().getRunState() == JobStatus.KILLED) {
              
              GroomProtocol worker = findGroomServer(tmpStatus);
              Directive d1 = new DispatchTasksDirective(
                  new GroomServerAction[] { new KillTaskAction(ts.getTaskId()) });
              try {
                worker.dispatch(d1);
              } catch (IOException ioe) {
                throw new DirectiveException("Error when dispatching kill task"
                    + " action.", ioe);
              }
            }
          }
        } else {
          throw new RuntimeException("BSPMaster contains GroomServerSatus, "
              + "but fail to retrieve it.");
        }
      } else {
        throw new RuntimeException("GroomServer not found."
            + groomStatus.getGroomName());
      }
    }
  }

  private class Instructor extends Thread {
    private final BlockingQueue<Directive> buffer = new LinkedBlockingQueue<Directive>();
    private final ConcurrentMap<Class<? extends Directive>, DirectiveHandler> handlers = new ConcurrentHashMap<Class<? extends Directive>, DirectiveHandler>();

    public void bind(Class<? extends Directive> instruction,
        DirectiveHandler handler) {
      handlers.putIfAbsent(instruction, handler);
    }

    public void put(Directive directive) {
      try {
        buffer.put(directive);
      } catch (InterruptedException ie) {
        LOG.error("Fail to put directive into queue.", ie);
      }
    }

    @Override
    public void run() {
      while (true) {
        try {
          Directive directive = this.buffer.take();
          handlers.get(directive.getClass()).handle(directive);
        } catch (InterruptedException ie) {
          LOG.error("Unable to retrieve directive from the queue.", ie);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          LOG.error("Fail to execute directive command.", e);
        }
      }
    }
  }

  /**
   * Start the BSPMaster process, listen on the indicated hostname/port
   * 
   * @param conf provides runtime parameters.
   */
  public BSPMaster(HamaConfiguration conf) throws IOException,
      InterruptedException {
    this(conf, generateNewIdentifier());
  }

  BSPMaster(HamaConfiguration conf, String identifier) throws IOException,
      InterruptedException {
    this.conf = conf;
    this.masterIdentifier = identifier;

    // Create the scheduler and init scheduler services
    Class<? extends TaskScheduler> schedulerClass = conf.getClass(
        "bsp.master.taskscheduler", SimpleTaskScheduler.class,
        TaskScheduler.class);
    this.taskScheduler = ReflectionUtils.newInstance(schedulerClass, conf);

    InetSocketAddress inetSocketAddress = getAddress(conf);
    // inetSocketAddress is null if we are in local mode, then we should start
    // nothing.
    if (inetSocketAddress != null) {
      host = inetSocketAddress.getHostName();
      port = inetSocketAddress.getPort();
      LOG.info("RPC BSPMaster: host " + host + " port " + port);

      startTime = System.currentTimeMillis();
      this.masterServer = RPC.getServer(this, host, port, conf);

      infoPort = conf.getInt("bsp.http.infoserver.port", 40013);

      infoServer = new HttpServer("bspmaster", host, infoPort, true, conf);
      infoServer.setAttribute("bsp.master", this);

      // starting webserver
      infoServer.start();

      if (conf.getBoolean("bsp.monitor.fd.enabled", false)) {
        this.supervisor.set(FDProvider.createSupervisor(conf.getClass(
            "bsp.monitor.fd.supervisor.class", UDPSupervisor.class,
            Supervisor.class), conf));
      }

      while (!Thread.currentThread().isInterrupted()) {
        try {
          if (fs == null) {
            fs = FileSystem.get(conf);
          }
        } catch (IOException e) {
          LOG.error("Can't get connection to Hadoop Namenode!", e);
        }
        try {
          // clean up the system dir, which will only work if hdfs is out of
          // safe mode
          if (systemDir == null) {
            systemDir = new Path(getSystemDir());
          }

          LOG.info("Cleaning up the system directory");
          LOG.info(systemDir);
          fs.delete(systemDir, true);
          if (FileSystem.mkdirs(fs, systemDir, new FsPermission(
              SYSTEM_DIR_PERMISSION))) {
            break;
          }
          LOG.error("Mkdirs failed to create " + systemDir);
          LOG.info(SUBDIR);

        } catch (AccessControlException ace) {
          LOG.warn("Failed to operate on bsp.system.dir (" + systemDir
              + ") because of permissions.");
          LOG.warn("Manually delete the bsp.system.dir (" + systemDir
              + ") and then start the BSPMaster.");
          LOG.warn("Bailing out ... ");
          throw ace;
        } catch (IOException ie) {
          LOG.info("problem cleaning system directory: " + systemDir, ie);
        }
        Thread.sleep(FS_ACCESS_RETRY_PERIOD);
      }

      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException();
      }

      deleteLocalFiles(SUBDIR);
    } else {
      System.out.println(localModeMessage);
      LOG.info(localModeMessage);
    }
  }

  /**
   * A GroomServer registers with its status to BSPMaster when startup, which
   * will update GroomServers cache.
   * 
   * @param status to be updated in cache.
   * @return true if registering successfully; false if fail.
   */
  @Override
  public boolean register(GroomServerStatus status) throws IOException {
    if (null == status) {
      LOG.error("No groom server status.");
      throw new NullPointerException("No groom server status.");
    }
    Throwable e = null;
    try {
      GroomProtocol wc = (GroomProtocol) RPC.waitForProxy(GroomProtocol.class,
          HamaRPCProtocolVersion.versionID,
          resolveWorkerAddress(status.getRpcServer()), this.conf);
      if (null == wc) {
        LOG.warn("Fail to create Worker client at host");
        return false;
      }
      // TODO: need to check if peer name has changed
      groomServers.putIfAbsent(status, wc);
    } catch (UnsupportedOperationException u) {
      e = u;
    } catch (ClassCastException c) {
      e = c;
    } catch (NullPointerException n) {
      e = n;
    } catch (IllegalArgumentException i) {
      e = i;
    } catch (Exception ex) {
      e = ex;
    }

    if (null != e) {
      LOG.error("Fail to register GroomServer " + status.getGroomName(), e);
      return false;
    }
    LOG.info(status.getGroomName() + " is added.");
    return true;
  }

  private static InetSocketAddress resolveWorkerAddress(String data) {
    return new InetSocketAddress(data.split(":")[0], Integer.parseInt(data
        .split(":")[1]));
  }

  private void updateGroomServersKey(GroomServerStatus old,
      GroomServerStatus newKey) {
    synchronized (groomServers) {
      GroomProtocol worker = groomServers.remove(old);
      groomServers.put(newKey, worker);
    }
  }

  @Override
  public boolean report(Directive directive) throws IOException {
    instructor.put(directive);
    return true;
  }

  // /////////////////////////////////////////////////////////////
  // BSPMaster methods
  // /////////////////////////////////////////////////////////////

  // Get the job directory in system directory
  Path getSystemDirectoryForJob(BSPJobID id) {
    return new Path(getSystemDir(), id.toString());
  }

  String[] getLocalDirs() throws IOException {
    return conf.getStrings("bsp.local.dir");
  }

  void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getLocal(conf).delete(new Path(localDirs[i]), true);
    }
  }

  void deleteLocalFiles(String subdir) throws IOException {
    try {
      String[] localDirs = getLocalDirs();
      for (int i = 0; i < localDirs.length; i++) {
        FileSystem.getLocal(conf).delete(new Path(localDirs[i], subdir), true);
      }
    } catch (NullPointerException e) {
      LOG.info(e);
    }
  }

  /**
   * Constructs a local file name. Files are distributed among configured local
   * directories.
   */
  Path getLocalPath(String pathString) throws IOException {
    return conf.getLocalPath("bsp.local.dir", pathString);
  }

  /**
   * Starts the BSP Master process.
   * @param conf The Hama configuration.
   * @return an instance of BSPMaster
   * @throws IOException
   * @throws InterruptedException
   */
  public static BSPMaster startMaster(HamaConfiguration conf)
      throws IOException, InterruptedException {
    return startMaster(conf, generateNewIdentifier());
  }

  /**
   * Starts the BSP Master process
   * @param conf The Hama configuration
   * @param identifier Identifier for the job.
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public static BSPMaster startMaster(HamaConfiguration conf, String identifier)
      throws IOException, InterruptedException {
    BSPMaster result = new BSPMaster(conf, identifier);
    // init zk root and child nodes
    // zk is required to be initialized before scheduler is started.
    result.initZK(conf);
    result.taskScheduler.setGroomServerManager(result);
    result.taskScheduler.setMonitorManager(result);
    if (conf.getBoolean("bsp.monitor.fd.enabled", false)) {
      result.supervisor.get().start();
    }
    result.taskScheduler.start();
    return result;
  }

  /**
   * Initialize the global synchronization client.
   * @param conf Hama configuration.
   */
  private void initZK(HamaConfiguration conf) {
    this.syncClient = new ZKSyncBSPMasterClient();
    this.syncClient.init(conf);
  }

  /**
   * Get a handle of the global synchronization client used by BSPMaster.
   * @return The synchronization client.
   */
  public MasterSyncClient getSyncClient(){
    return this.syncClient;
  }

  /**
   * Parses the configuration for the master addresses.
   * 
   * @param conf normal configuration containing the information the user
   *          configured.
   * @return a InetSocketAddress if everything went fine. Or "null" if "local"
   *         was configured, which is no valid hostname.
   */
  public static InetSocketAddress getAddress(Configuration conf) {
    String hamaMasterStr = conf.get("bsp.master.address", "localhost");
    // we ensure that hamaMasterStr is non-null here because we provided
    // "localhost" as default.
    if (!hamaMasterStr.equals("local")) {
      int defaultPort = conf.getInt("bsp.master.port", 40000);
      return NetUtils.createSocketAddr(hamaMasterStr, defaultPort);
    } else {
      return null;
    }
  }

  /**
   * BSPMaster identifier
   * 
   * @return String BSPMaster identification number
   */
  private static String generateNewIdentifier() {
    return new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
  }

  public void offerService() throws InterruptedException, IOException {

    this.masterServer.start();

    state.set(State.RUNNING);

    instructor = new Instructor();
    instructor.bind(ReportGroomStatusDirective.class,
        new ReportGroomStatusHandler());
    instructor.start();

    LOG.info("Starting RUNNING");

    this.masterServer.join();

    LOG.info("Stopped RPC Master server.");
  }

  // //////////////////////////////////////////////////
  // InterServerProtocol
  // //////////////////////////////////////////////////
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(MasterProtocol.class.getName())) {
      return HamaRPCProtocolVersion.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return HamaRPCProtocolVersion.versionID;
    } else {
      throw new IOException("Unknown protocol to BSPMaster: " + protocol);
    }
  }

  // //////////////////////////////////////////////////
  // JobSubmissionProtocol
  // //////////////////////////////////////////////////
  /**
   * This method returns new job id. The returned job id increases sequentially.
   */
  @Override
  public BSPJobID getNewJobId() throws IOException {
    int id;
    synchronized (nextJobId) {
      id = nextJobId;
      nextJobId = Integer.valueOf(id + 1);
    }
    return new BSPJobID(this.masterIdentifier, id);
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException {
    if (jobs.containsKey(jobID)) {
      // job already running, don't start twice
      LOG.info("The job (" + jobID + ") was already submitted");
      return jobs.get(jobID).getStatus();
    }

    JobInProgress job = new JobInProgress(jobID, new Path(jobFile), this,
        this.conf);
    ++totalSubmissions;
    if(LOG.isDebugEnabled()){
      LOG.debug("Submitting job number = " + totalSubmissions + 
          " id = " + job.getJobID());
    }
    return addJob(jobID, job);
  }

  // //////////////////////////////////////////////////
  // GroomServerManager functions
  // //////////////////////////////////////////////////

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) {
    Map<String, GroomServerStatus> groomsMap = null;

    // give the caller a snapshot of the cluster status
    int numGroomServers = groomServers.size();
    if (detailed) {
      groomsMap = new HashMap<String, GroomServerStatus>();
      for (Map.Entry<GroomServerStatus, GroomProtocol> entry : groomServers
          .entrySet()) {
        GroomServerStatus s = entry.getKey();
        groomsMap.put(s.getGroomHostName() + ":"
            + Constants.DEFAULT_GROOM_INFO_SERVER, s);
      }
    }

    int tasksPerGroom = conf.getInt(Constants.MAX_TASKS_PER_GROOM, 3);
    this.totalTaskCapacity = tasksPerGroom * numGroomServers;

    if (detailed) {
      return new ClusterStatus(groomsMap, totalTasks, totalTaskCapacity,
          state.get());
    } else {
      return new ClusterStatus(numGroomServers, totalTasks, totalTaskCapacity,
          state.get());
    }
  }

  @Override
  public GroomProtocol findGroomServer(GroomServerStatus status) {
    return groomServers.get(status);
  }

  @Override
  public Collection<GroomProtocol> findGroomServers() {
    return groomServers.values();
  }

  @Override
  public Collection<GroomServerStatus> groomServerStatusKeySet() {
    return groomServers.keySet();
  }

  @Override
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  @Override
  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }

  @Override
  public void moveToBlackList(String host) {
    LOG.info("[moveToBlackList()]Host to be moved to black list: " + host);
    for (GroomServerStatus groomStatus : groomServerStatusKeySet()) {
      LOG.info("[moveToBlackList()]GroomServerStatus's host name:"
          + groomStatus.getGroomHostName() + " host:" + host);
      if (groomStatus.getGroomHostName().equals(host)) {
        boolean result = groomServers.remove(groomStatus,
            findGroomServer(groomStatus));
        if (!result) {
          LOG.error("Fail to remove " + host + " out of groom server cache!");
        }
        blackList.add(groomStatus);
        LOG.info("[moveToBlackList()] " + host
            + " is successfully moved to black list.");
      }
    }
  }

  @Override
  public void removeOutOfBlackList(String host) {
    for (GroomServerStatus groomStatus : blackList) {
      if (groomStatus.getGroomHostName().equals(host)) {
        boolean result = blackList.remove(groomStatus);
        if (result)
          LOG.info("Successfully remove " + host + " out of black list.");
        else
          LOG.error("Fail to remove " + host + " out of black list.");
      }
    }
  }

  @Override
  public Collection<GroomServerStatus> alive() {
    return groomServerStatusKeySet();
  }

  @Override
  public Collection<GroomServerStatus> blackList() {
    return blackList;
  }

  @Override
  public GroomServerStatus findInBlackList(String host) {
    for (GroomServerStatus status : blackList) {
      if (host.equals(status.getGroomHostName())) {
        return status;
      }
    }
    return null;
  }

  public String getBSPMasterName() {
    return host + ":" + port;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getBSPMasterIdentifier() {
    return masterIdentifier;
  }

  public int getHttpPort() {
    return infoPort;
  }

  /**
   * Adds a job to the bsp master. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * 
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(BSPJobID jobId, JobInProgress job) {
    synchronized (jobs) {
      jobs.put(job.getProfile().getJobID(), job);
      for (JobInProgressListener listener : jobInProgressListeners) {
        try {
          listener.jobAdded(job);
        } catch (IOException ioe) {
          LOG.error("Fail to alter Scheduler a job is added.", ioe);
        }
      }
    }
    return job.getStatus();
  }
  
  /**
   * Recovers task in job. To be called when a particular task in a job has failed 
   * and there is a need to schedule it on a machine.
   */
  private synchronized void recoverTask(JobInProgress job) {
    ++totalSubmissions;
    for (JobInProgressListener listener : jobInProgressListeners) {
      try {
        listener.recoverTaskInJob(job);
      } catch (IOException ioe) {
        LOG.error("Fail to alter Scheduler a job is added.", ioe);
      }
    }
  }

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    return getJobStatus(jobs.values(), true);
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    LOG.debug("returns all jobs: " + jobs.size());
    return getJobStatus(jobs.values(), false);
  }

  private synchronized static JobStatus[] getJobStatus(
      Collection<JobInProgress> jips, boolean toComplete) {
    if (jips == null) {
      return new JobStatus[] {};
    }
    List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for (JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();

      status.setStartTime(jip.getStartTime());
      status.setNumOfTasks(jip.getNumOfTasks());

      // Sets the user name
      status.setUsername(jip.getProfile().getUser());
      status.setName(jip.getJobName());

      if (toComplete) {
        if (status.getRunState() == JobStatus.RUNNING
            || status.getRunState() == JobStatus.PREP) {
          jobStatusList.add(status);
        }
      } else {
        jobStatusList.add(status);
      }
    }

    return jobStatusList.toArray(new JobStatus[jobStatusList.size()]);
  }

  @Override
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }

  /**
   * Return system directory to which BSP store control files.
   */
  @Override
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system"));
    return fs.makeQualified(sysDir).toString();
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        return job.getProfile();
      }
    }
    return null;
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        return job.getStatus();
      }
    }
    return null;
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    JobInProgress job = jobs.get(jobid);

    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }

    killJob(job);
  }

  private synchronized static void killJob(JobInProgress job) {
    LOG.info("Killing job " + job.getJobID());
    job.kill();
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    return false;
  }

  public static BSPMaster constructMaster(
      Class<? extends BSPMaster> masterClass, final Configuration conf) {
    try {
      Constructor<? extends BSPMaster> c = masterClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Master: "
          + masterClass.toString()
          + ((e.getCause() != null) ? e.getCause().getMessage() : ""), e);
    }
  }

  /**
   * Shuts down the BSP Process and does the necessary clean up.
   */
  public void shutdown() {
    try {
      this.syncClient.close();
    } catch (IOException e) {
      LOG.error("Error closing the sync client",e);
    }
    if (null != this.supervisor.get()) {
      this.supervisor.get().stop();
    }
    this.masterServer.stop();
  }

  public BSPMaster.State currentState() {
    return this.state.get();
  }

  @Override
  public void process(WatchedEvent event) {
  }

  @Override
  public Supervisor supervisor() {
    return this.supervisor.get();
  }

  TaskCompletionEvent[] EMPTY_EVENTS = new TaskCompletionEvent[0];

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(BSPJobID jobid,
      int fromEventId, int maxEvents) {
    synchronized (this) {
      JobInProgress job = this.jobs.get(jobid);
      if (null != job) {
        if (job.areTasksInited()) {
          return job.getTaskCompletionEvents(fromEventId, maxEvents);
        } else {
          return EMPTY_EVENTS;
        }
      }
    }
    return null;
  }
}
