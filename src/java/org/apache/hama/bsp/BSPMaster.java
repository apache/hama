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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

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
import org.apache.hama.ipc.InterServerProtocol;
import org.apache.hama.ipc.JobSubmissionProtocol;

/**
 * BSPMaster is responsible to control all the groom servers and to manage bsp
 * jobs.
 */
public class BSPMaster implements JobSubmissionProtocol, InterServerProtocol,
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
  private Server interServer;

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

  // Groom Servers
  // (groom name --> last sent HeartBeatResponse)
  Map<String, HeartbeatResponse> groomToHeartbeatResponseMap = new TreeMap<String, HeartbeatResponse>();
  private HashMap<String, GroomServerStatus> groomServers = new HashMap<String, GroomServerStatus>();
  // maps groom server names to peer names
  private HashMap<String, String> groomServerPeers = new HashMap<String, String>();

  // Jobs' Meta Data
  private Integer nextJobId = Integer.valueOf(1);
  // private long startTime;
  private int totalSubmissions = 0;
  private int totalTasks = 0;
  private int totalTaskCapacity;
  private Map<BSPJobID, JobInProgress> jobs = new TreeMap<BSPJobID, JobInProgress>();
  private TaskScheduler taskScheduler;

  TreeMap<TaskAttemptID, String> taskIdToGroomNameMap = new TreeMap<TaskAttemptID, String>();
  TreeMap<String, TreeSet<TaskAttemptID>> groomNameToTaskIdsMap = new TreeMap<String, TreeSet<TaskAttemptID>>();
  Map<TaskAttemptID, TaskInProgress> taskIdToTaskInProgressMap = new TreeMap<TaskAttemptID, TaskInProgress>();

  Vector<JobInProgress> jobInitQueue = new Vector<JobInProgress>();
  JobInitThread initJobs = new JobInitThread();

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

    // Create the scheduler
    Class<? extends TaskScheduler> schedulerClass = conf.getClass(
        "bsp.master.taskscheduler", SimpleTaskScheduler.class,
        TaskScheduler.class);
    this.taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(
        schedulerClass, conf);

    InetSocketAddress addr = getAddress(conf);
    this.interServer = RPC.getServer(this, addr.getHostName(), addr
        .getPort(), conf);

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

  // /////////////////////////////////////////////////////
  // Accessors for objects that want info on jobs, tasks,
  // grooms, etc.
  // /////////////////////////////////////////////////////
  public GroomServerStatus getGroomServer(String groomID) {
    synchronized (groomServers) {
      return groomServers.get(groomID);
    }
  }

  public List<String> groomServerNames() {
    List<String> activeGrooms = new ArrayList<String>();
    synchronized (groomServers) {
      for (GroomServerStatus status : groomServers.values()) {
        activeGrooms.add(status.getGroomName());
      }
    }
    return activeGrooms;
  }

  // ///////////////////////////////////////////////////////////////
  // Used to init new jobs that have just been created
  // ///////////////////////////////////////////////////////////////
  class JobInitThread implements Runnable {
    private volatile boolean shouldRun = true;

    public JobInitThread() {
    }

    public void run() {
      while (shouldRun) {
        JobInProgress job = null;
        synchronized (jobInitQueue) {
          if (jobInitQueue.size() > 0) {
            job = (JobInProgress) jobInitQueue.elementAt(0);
            jobInitQueue.remove(job);
          } else {
            try {
              jobInitQueue.wait(JOBINIT_SLEEP_INTERVAL);
            } catch (InterruptedException iex) {
            }
          }
        }
        try {
          if (job != null) {
            job.initTasks();
          }
        } catch (Exception e) {
          LOG.warn("job init failed: " + e);
          job.kill();
        }
      }
    }

    public void stopIniter() {
      shouldRun = false;
    }
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

    return result;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String hamaMasterStr = conf.get("bsp.master.address", "localhost");
    int defaultPort = conf.getInt("bsp.master.port", 40000);

    return NetUtils.createSocketAddr(hamaMasterStr, defaultPort);
  }

  /**
   * 
   * @return
   */
  private static String generateNewIdentifier() {
    return new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
  }

  public void offerService() throws InterruptedException, IOException {
    new Thread(this.initJobs).start();
    LOG.info("Starting jobInitThread");

    this.interServer.start();

    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");

    this.interServer.join();
    LOG.info("Stopped interServer");
  }

  // //////////////////////////////////////////////////
  // InterServerProtocol
  // //////////////////////////////////////////////////
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(InterServerProtocol.class.getName())) {
      return InterServerProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to BSPMaster: " + protocol);
    }
  }

  /**
   * A RPC method for transmitting each peer status from peer to master.
   * 
   * @throws IOException
   */
  @Override
  public HeartbeatResponse heartbeat(GroomServerStatus status,
      boolean restarted, boolean initialContact, boolean acceptNewTasks,
      short responseId, int reportSize) throws IOException {

    // First check if the last heartbeat response got through
    String groomName = status.getGroomName();
    long now = System.currentTimeMillis();

    HeartbeatResponse prevHeartbeatResponse = groomToHeartbeatResponseMap
        .get(groomName);

    // Process this heartbeat
    short newResponseId = (short) (responseId + 1);
    status.setLastSeen(now);
    if (!processHeartbeat(status, initialContact)) {
      if (prevHeartbeatResponse != null) {
        groomToHeartbeatResponseMap.remove(groomName);
      }
      return new HeartbeatResponse(newResponseId,
          new GroomServerAction[] { new ReinitGroomAction() }, Collections
              .<String, String> emptyMap());
    }

    HeartbeatResponse response = new HeartbeatResponse(newResponseId, null,
        groomServerPeers);
    List<GroomServerAction> actions = new ArrayList<GroomServerAction>();

    // Check for new tasks to be executed on the groom server
    if (acceptNewTasks) {
      GroomServerStatus groomStatus = getGroomServer(groomName);
      if (groomStatus == null) {
        LOG.warn("Unknown groom server polling; ignoring: " + groomName);
      } else {
        List<Task> taskList = taskScheduler.assignTasks(groomStatus);

        for (Task task : taskList) {
          if (task != null) {
            actions.add(new LaunchTaskAction(task));
          }
        }
      }
    }

    response.setActions(actions.toArray(new GroomServerAction[actions.size()]));

    groomToHeartbeatResponseMap.put(groomName, response);
    removeMarkedTasks(groomName);
    updateTaskStatuses(status);

    return response;
  }

  void updateTaskStatuses(GroomServerStatus status) {
    for (Iterator<TaskStatus> it = status.taskReports(); it.hasNext();) {
      TaskStatus report = it.next();
      report.setGroomServer(status.getGroomName());
      TaskAttemptID taskId = report.getTaskId();
      TaskInProgress tip = (TaskInProgress) taskIdToTaskInProgressMap
          .get(taskId);

      if (tip == null) {
        LOG.info("Serious problem.  While updating status, cannot find taskid "
            + report.getTaskId());
      } else {
        JobInProgress job = tip.getJob();

        if (report.getRunState() == TaskStatus.State.SUCCEEDED) {
          job.completedTask(tip, report);
        } else if (report.getRunState() == TaskStatus.State.FAILED) {
          // TODO Tell the job to fail the relevant task

        } else {
          job.updateTaskStatus(tip, report);
        }
      }

    }
  }

  // (trackerID -> TreeSet of completed taskids running at that tracker)
  TreeMap<String, TreeSet<TaskAttemptID>> trackerToMarkedTasksMap = new TreeMap<String, TreeSet<TaskAttemptID>>();

  private void removeMarkedTasks(String groomName) {
    // Purge all the 'marked' tasks which were running at groomServer
    TreeSet<TaskAttemptID> markedTaskSet = trackerToMarkedTasksMap
        .get(groomName);
    if (markedTaskSet != null) {
      for (TaskAttemptID taskid : markedTaskSet) {
        removeTaskEntry(taskid);
        LOG.info("Removed completed task '" + taskid + "' from '" + groomName
            + "'");
      }
      // Clear
      trackerToMarkedTasksMap.remove(groomName);
    }
  }

  private void removeTaskEntry(TaskAttemptID taskid) {
    // taskid --> groom
    String groom = taskIdToGroomNameMap.remove(taskid);

    // groom --> taskid
    if (groom != null) {
      TreeSet<TaskAttemptID> groomSet = groomNameToTaskIdsMap.get(groom);
      if (groomSet != null) {
        groomSet.remove(taskid);
      }
    }

    // taskid --> TIP
    taskIdToTaskInProgressMap.remove(taskid);
    LOG.debug("Removing task '" + taskid + "'");
  }

  private List<GroomServerAction> getTasksToKill(String groomName) {
    Set<TaskAttemptID> taskIds = groomNameToTaskIdsMap.get(groomName);
    if (taskIds != null) {
      List<GroomServerAction> killList = new ArrayList<GroomServerAction>();
      Set<String> killJobIds = new TreeSet<String>();
      for (TaskAttemptID killTaskId : taskIds) {
        TaskInProgress tip = (TaskInProgress) taskIdToTaskInProgressMap
            .get(killTaskId);
        if (tip.shouldCloseForClosedJob(killTaskId)) {
          // 
          // This is how the BSPMaster ends a task at the GroomServer.
          // It may be successfully completed, or may be killed in
          // mid-execution.
          //
          if (tip.getJob().getStatus().getRunState() == JobStatus.RUNNING) {
            killList.add(new KillTaskAction(killTaskId));
            LOG.debug(groomName + " -> KillTaskAction: " + killTaskId);
          } else {
            String killJobId = tip.getJob().getStatus().getJobID()
                .getJtIdentifier();
            killJobIds.add(killJobId);
          }
        }
      }

      for (String killJobId : killJobIds) {
        killList.add(new KillJobAction(killJobId));
        LOG.debug(groomName + " -> KillJobAction: " + killJobId);
      }

      return killList;
    }
    return null;

  }

  /**
   * Process incoming heartbeat messages from the groom.
   */
  private synchronized boolean processHeartbeat(GroomServerStatus groomStatus,
      boolean initialContact) {
    String groomName = groomStatus.getGroomName();

    synchronized (groomServers) {
      GroomServerStatus oldStatus = groomServers.get(groomName);
      if (oldStatus == null) {
        groomServers.put(groomName, groomStatus);
      } else { // TODO - to be improved to update status.
      }
    }

    if (initialContact) {
      groomServerPeers.put(groomStatus.getGroomName(), groomStatus
          .getPeerName());
    }

    return true;
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

    JobInProgress job = new JobInProgress(jobID, this, this.conf);
    return addJob(jobID, job);
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) {
    int numGroomServers;
    Map<String, String> groomPeersMap = null;

    // give the caller a snapshot of the cluster status
    synchronized (this) {
      numGroomServers = groomServerPeers.size();
      if (detailed) {
        groomPeersMap = new HashMap<String, String>(groomServerPeers);
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

  /**
   * Adds a job to the bsp master. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * 
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(BSPJobID jodId, JobInProgress job) {
    totalSubmissions++;
    synchronized (jobs) {
      synchronized (jobInitQueue) {
        jobs.put(job.getProfile().getJobID(), job);
        taskScheduler.addJob(job);
        jobInitQueue.add(job);
        jobInitQueue.notifyAll();
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
    this.interServer.stop();
  }

  public void createTaskEntry(TaskAttemptID taskid, String groomServer,
      TaskInProgress taskInProgress) {
    LOG.info("Adding task '" + taskid + "' to tip " + taskInProgress.getTIPId()
        + ", for groom '" + groomServer + "'");

    // taskid --> groom
    taskIdToGroomNameMap.put(taskid, groomServer);

    // groom --> taskid
    TreeSet<TaskAttemptID> taskset = groomNameToTaskIdsMap.get(groomServer);
    if (taskset == null) {
      taskset = new TreeSet<TaskAttemptID>();
      groomNameToTaskIdsMap.put(groomServer, taskset);
    }
    taskset.add(taskid);

    // taskid --> TIP
    taskIdToTaskInProgressMap.put(taskid, taskInProgress);
  }
}
