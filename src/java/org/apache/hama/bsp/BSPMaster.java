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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.ipc.WorkerProtocol;

/**
 * BSPMaster is responsible to control all the groom servers and to manage bsp
 * jobs.
 */
public class BSPMaster implements JobSubmissionProtocol, MasterProtocol, // InterServerProtocol,
    GroomServerManager {
  public static final Log LOG = LogFactory.getLog(BSPMaster.class);

  private HamaConfiguration conf;

  // Constants
  public static enum State {
    INITIALIZING, RUNNING
  }

  private static final int FS_ACCESS_RETRY_PERIOD = 10000;
  public static final long GROOMSERVER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long JOBINIT_SLEEP_INTERVAL = 2000;

  // States
  State state = State.INITIALIZING;

  // Attributes
  String masterIdentifier;
  // private Server interServer;
  private Server masterServer;

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
  // private long startTime;
  private int totalSubmissions = 0; // how many jobs has been submitted by
  // clients
  private int totalTasks = 0; // currnetly running tasks
  private int totalTaskCapacity; // max tasks that groom server can run

  private Map<BSPJobID, JobInProgress> jobs = new TreeMap<BSPJobID, JobInProgress>();
  private TaskScheduler taskScheduler;

  // GroomServers cache
  protected ConcurrentMap<GroomServerStatus, WorkerProtocol> groomServers = new ConcurrentHashMap<GroomServerStatus, WorkerProtocol>();

  private final List<JobInProgressListener> jobInProgressListeners = new CopyOnWriteArrayList<JobInProgressListener>();

  /**
   * Start the BSPMaster process, listen on the indicated hostname/port
   */
  public BSPMaster(HamaConfiguration conf) throws IOException,
      InterruptedException {
    this(conf, generateNewIdentifier());
  }

  BSPMaster(HamaConfiguration conf, String identifier) throws IOException,
      InterruptedException {
    this.conf = conf;
    this.masterIdentifier = identifier;
    // expireLaunchingTaskThread.start();

    // Create the scheduler and init scheduler services
    Class<? extends TaskScheduler> schedulerClass = conf.getClass(
        "bsp.master.taskscheduler", SimpleTaskScheduler.class,
        TaskScheduler.class);
    this.taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(
        schedulerClass, conf);

    String host = getAddress(conf).getHostName();
    int port = getAddress(conf).getPort();
    LOG.info("RPC BSPMaster: host " + host + " port " + port);
    this.masterServer = RPC.getServer(this, host, port, conf);

    while (!Thread.currentThread().isInterrupted()) {
      try {
        if (fs == null) {
          fs = FileSystem.get(conf);
        }
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
      WorkerProtocol wc = (WorkerProtocol) RPC.waitForProxy(
          WorkerProtocol.class, WorkerProtocol.versionID,
          resolveWorkerAddress(status.getRpcServer()), this.conf);
      if (null == wc) {
        LOG.warn("Fail to create Worker client at host " + status.getPeerName());
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

    return true;
  }

  private static InetSocketAddress resolveWorkerAddress(String data) {
    return new InetSocketAddress(data.split(":")[0], Integer.parseInt(data
        .split(":")[1]));
  }

  private void updateGroomServersKey(GroomServerStatus old,
      GroomServerStatus newKey) {
    synchronized (groomServers) {
      WorkerProtocol worker = groomServers.remove(old);
      groomServers.put(newKey, worker);
    }
  }

  @Override
  public boolean report(Directive directive) throws IOException {
    // check returned directive type if equals response
    if (directive.getType().value() != Directive.Type.Response.value()) {
      throw new IllegalStateException("GroomServer should report()"
          + " with Response. Current report type:" + directive.getType());
    }
    // update GroomServerStatus hold in groomServers cache.
    GroomServerStatus fstus = directive.getStatus();

    // groomServers cache contains groom server status reported back
    if (groomServers.containsKey(fstus)) {
      GroomServerStatus ustus = null;
      for (GroomServerStatus old : groomServers.keySet()) {
        if (old.equals(fstus)) {
          ustus = fstus;
          updateGroomServersKey(old, ustus);
          break;
        }
      }// for

      if (null != ustus) {
        List<TaskStatus> tlist = ustus.getTaskReports();
        for (TaskStatus ts : tlist) {
          JobInProgress jip = whichJob(ts.getJobId());

          TaskInProgress tip = jip.findTaskInProgress(((TaskAttemptID) ts
              .getTaskId()).getTaskID());
          
          if(ts.getRunState() == TaskStatus.State.SUCCEEDED) {
            jip.completedTask(tip, ts);
          } else if (ts.getRunState() == TaskStatus.State.RUNNING) {
            // do nothing
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
            jip.getStatus().setprogress(ts.getSuperstepCount());
          }
        }
      } else {
        throw new RuntimeException("BSPMaster contains GroomServerSatus, "
            + "but fail to retrieve it.");
      }
    } else {
      throw new RuntimeException("GroomServer not found."
          + fstus.getGroomName());
    }
    return true;
  }

  private JobInProgress whichJob(BSPJobID id) {
    for (JobInProgress job : taskScheduler
        .getJobs(SimpleTaskScheduler.PROCESSING_QUEUE)) {
      if (job.getJobID().equals(id)) {
        return job;
      }
    }
    return null;
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

  public static BSPMaster startMaster(HamaConfiguration conf)
      throws IOException, InterruptedException {
    return startMaster(conf, generateNewIdentifier());
  }

  public static BSPMaster startMaster(HamaConfiguration conf, String identifier)
      throws IOException, InterruptedException {

    BSPMaster result = new BSPMaster(conf, identifier);
    result.taskScheduler.setGroomServerManager(result);
    result.taskScheduler.start();

    return result;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String hamaMasterStr = conf.get("bsp.master.address", "localhost");
    int defaultPort = conf.getInt("bsp.master.port", 40000);

    return NetUtils.createSocketAddr(hamaMasterStr, defaultPort);
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
    // this.interServer.start();
    this.masterServer.start();

    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");

    // this.interServer.join();
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
      return MasterProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return JobSubmissionProtocol.versionID;
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
    return addJob(jobID, job);
  }

  // //////////////////////////////////////////////////
  // GroomServerManager functions
  // //////////////////////////////////////////////////

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) {
    Map<String, String> groomPeersMap = null;

    // give the caller a snapshot of the cluster status
    int numGroomServers = groomServers.size();
    if (detailed) {
      groomPeersMap = new HashMap<String, String>();
      for (Map.Entry<GroomServerStatus, WorkerProtocol> entry : groomServers
          .entrySet()) {
        GroomServerStatus s = entry.getKey();
        groomPeersMap.put(s.getGroomName(), s.getPeerName());
      }
    }
    if (detailed) {
      return new ClusterStatus(groomPeersMap, totalTasks, totalTaskCapacity,
          state);
    } else {
      return new ClusterStatus(numGroomServers, totalTasks, totalTaskCapacity,
          state);
    }
  }

  @Override
  public WorkerProtocol findGroomServer(GroomServerStatus status) {
    return groomServers.get(status);
  }

  @Override
  public Collection<WorkerProtocol> findGroomServers() {
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
  public Map<String, String> currentGroomServerPeers() {
    Map<String, String> tmp = new HashMap<String, String>();
    for (GroomServerStatus status : groomServers.keySet()) {
      tmp.put(status.getGroomName(), status.getPeerName());
    }
    return tmp;
  }

  /**
   * Adds a job to the bsp master. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * 
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(BSPJobID jobId, JobInProgress job) {
    totalSubmissions++;
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

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    return getJobStatus(jobs.values(), true);
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    LOG.debug("returns all jobs: " + jobs.size());
    return getJobStatus(jobs.values(), false);
  }

  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if (jips == null) {
      return new JobStatus[] {};
    }
    List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for (JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();

      status.setStartTime(jip.getStartTime());
      // Sets the user name
      status.setUsername(jip.getProfile().getUser());

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

  private synchronized void killJob(JobInProgress job) {
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

  public void shutdown() {
    this.masterServer.stop();
  }

  public BSPMaster.State currentState() {
    return this.state;
  }
}
