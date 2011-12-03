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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.BSPMaster.State;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.MessageManagerFactory;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.hama.util.BSPNetUtils;

/**
 * A multithreaded local BSP runner that can be used for debugging and local
 * running BSP's.
 */
public class LocalBSPRunner implements JobSubmissionProtocol {
  public static final Log LOG = LogFactory.getLog(LocalBSPRunner.class);

  private static final String IDENTIFIER = "localrunner";
  private static String WORKING_DIR = "/tmp/hama-bsp/";
  protected static volatile ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors
      .newCachedThreadPool();

  @SuppressWarnings("rawtypes")
  protected static final LinkedList<Future<BSP>> futureList = new LinkedList<Future<BSP>>();

  protected String jobFile;
  protected String jobName;

  protected JobStatus currentJobStatus;

  protected Configuration conf;
  protected FileSystem fs;

  private static long superStepCount = 0L;
  private static String[] peerNames;

  // this is used for not-input driven job
  private int maxTasks;

  public LocalBSPRunner(Configuration conf) throws IOException {
    super();
    this.conf = conf;

    maxTasks = conf.getInt("bsp.local.tasks.maximum", 20);

    String path = conf.get("bsp.local.dir");
    if (path != null && !path.isEmpty()) {
      WORKING_DIR = path;
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return 3;
  }

  @Override
  public BSPJobID getNewJobId() throws IOException {
    return new BSPJobID(IDENTIFIER, 1);
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException {
    this.jobFile = jobFile;

    if (fs == null) {
      this.fs = FileSystem.get(conf);
    }

    // add the resource to the current configuration, because add resouce in
    // HamaConfigurations constructor (ID,FILE) does not take local->HDFS
    // connections into account. This leads to not serializing the
    // configuration, which yields into failure.
    conf.addResource(fs.open(new Path(jobFile)));

    conf.setClass(MessageManagerFactory.MESSAGE_MANAGER_CLASS,
        LocalMessageManager.class, MessageManager.class);
    conf.setClass(SyncServiceFactory.SYNC_CLIENT_CLASS, LocalSyncClient.class,
        SyncClient.class);

    BSPJob job = new BSPJob(new HamaConfiguration(conf), jobID);
    currentJobStatus = new JobStatus(jobID, System.getProperty("user.name"),
        0L, JobStatus.RUNNING);

    int numBspTask = job.getNumBspTask();

    String jobSplit = conf.get("bsp.job.split.file");

    BSPJobClient.RawSplit[] splits = null;
    if (jobSplit != null) {

      DataInputStream splitFile = fs.open(new Path(jobSplit));

      try {
        splits = BSPJobClient.readSplitFile(splitFile);
      } finally {
        splitFile.close();
      }
    }

    peerNames = new String[numBspTask];
    for (int i = 0; i < numBspTask; i++) {
      peerNames[i] = "local:" + i;
      futureList.add(threadPool.submit(new BSPRunner(new Configuration(conf),
          job, i, splits)));
    }

    new Thread(new ThreadObserver(currentJobStatus)).start();
    return currentJobStatus;
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    return new ClusterStatus(maxTasks, maxTasks, maxTasks, State.RUNNING);
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    return new JobProfile(System.getProperty("user.name"), jobid, jobFile,
        jobName);
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    currentJobStatus.setSuperstepCount(superStepCount);
    currentJobStatus.setprogress(superStepCount);
    return currentJobStatus;
  }

  @Override
  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    return null;
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    return null;
  }

  @Override
  public String getSystemDir() {
    return WORKING_DIR;
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    return;
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    return false;
  }

  // this class will spawn a new thread and executes the BSP
  @SuppressWarnings( { "deprecation", "rawtypes" })
  static class BSPRunner implements Callable<BSP> {

    private Configuration conf;
    private BSPJob job;
    private int id;
    private BSP bsp;
    private RawSplit[] splits;
    private static final Counters counters = new Counters();

    public BSPRunner(Configuration conf, BSPJob job, int id, RawSplit[] splits) {
      super();
      this.conf = conf;
      this.job = job;
      this.id = id;
      this.splits = splits;

      // set the peer port to the id, to prevent collision
      conf.setInt(Constants.PEER_PORT, id);
      conf.set(Constants.PEER_HOST, "local");

      bsp = (BSP) ReflectionUtils.newInstance(job.getConf().getClass(
          "bsp.work.class", BSP.class), job.getConf());

    }

    // deprecated until 0.5.0, then it will be removed.
    @SuppressWarnings("unchecked")
    public void run() throws Exception {

      String splitname = null;
      BytesWritable realBytes = null;
      if (splits != null) {
        splitname = splits[id].getClassName();
        realBytes = splits[id].getBytes();
      }

      BSPPeerImpl peer = new BSPPeerImpl(job, conf, new TaskAttemptID(
          new TaskID(job.getJobID(), id), id), new LocalUmbilical(), id,
          splitname, realBytes, counters);

      bsp.setConf(conf);
      try {
        bsp.setup(peer);
        bsp.bsp(peer);
      } catch (Exception e) {
        LOG.error("Exception during BSP execution!", e);
      }
      bsp.cleanup(peer);
      peer.clear();
      peer.close();
    }

    @Override
    public BSP call() throws Exception {
      run();
      return bsp;
    }
  }

  // this thread observes the status of the runners.
  class ThreadObserver implements Runnable {

    JobStatus status;

    public ThreadObserver(JobStatus currentJobStatus) {
      this.status = currentJobStatus;
    }

    @Override
    public void run() {
      boolean success = true;
      for (@SuppressWarnings("rawtypes")
      Future<BSP> future : futureList) {
        try {
          future.get();
        } catch (InterruptedException e) {
          LOG.error("Exception during BSP execution!", e);
          success = false;
        } catch (ExecutionException e) {
          LOG.error("Exception during BSP execution!", e);
          success = false;
        }
      }
      if (success) {
        currentJobStatus.setState(JobStatus.State.SUCCEEDED);
        currentJobStatus.setRunState(JobStatus.SUCCEEDED);
      } else {
        currentJobStatus.setState(JobStatus.State.FAILED);
        currentJobStatus.setRunState(JobStatus.FAILED);
      }
      threadPool.shutdownNow();
    }

  }

  public static class LocalMessageManager implements MessageManager {

    private static final ConcurrentHashMap<InetSocketAddress, LocalMessageManager> managerMap = new ConcurrentHashMap<InetSocketAddress, LocalBSPRunner.LocalMessageManager>();

    private final HashMap<InetSocketAddress, LinkedList<BSPMessage>> localOutgoingMessages = new HashMap<InetSocketAddress, LinkedList<BSPMessage>>();
    private static final ConcurrentHashMap<String, InetSocketAddress> socketCache = new ConcurrentHashMap<String, InetSocketAddress>();
    private final LinkedBlockingDeque<BSPMessage> localIncomingMessages = new LinkedBlockingDeque<BSPMessage>();

    @Override
    public void init(Configuration conf, InetSocketAddress peerAddress) {
      managerMap.put(peerAddress, this);
    }

    @Override
    public void close() {

    }

    @Override
    public BSPMessage getCurrentMessage() throws IOException {
      if (localIncomingMessages.isEmpty()) {
        return null;
      } else {
        return localIncomingMessages.pop();
      }
    }

    @Override
    public void send(String peerName, BSPMessage msg) throws IOException {
      InetSocketAddress inetSocketAddress = socketCache.get(peerName);
      if (inetSocketAddress == null) {
        inetSocketAddress = BSPNetUtils.getAddress(peerName);
        socketCache.put(peerName, inetSocketAddress);
      }
      LinkedList<BSPMessage> msgs = localOutgoingMessages
          .get(inetSocketAddress);
      if (msgs == null) {
        msgs = new LinkedList<BSPMessage>();
      }
      msgs.add(msg);

      localOutgoingMessages.put(inetSocketAddress, msgs);
    }

    @Override
    public Iterator<Entry<InetSocketAddress, LinkedList<BSPMessage>>> getMessageIterator() {
      return localOutgoingMessages.entrySet().iterator();
    }

    @Override
    public void transfer(InetSocketAddress addr, BSPMessageBundle bundle)
        throws IOException {
      for (BSPMessage value : bundle.getMessages()) {
        managerMap.get(addr).localIncomingMessages.add(value);
      }
    }

    @Override
    public void clearOutgoingQueues() {
      localOutgoingMessages.clear();
    }

    @Override
    public int getNumCurrentMessages() {
      return localIncomingMessages.size();
    }

  }

  public static class LocalUmbilical implements BSPPeerProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Task getTask(TaskAttemptID taskid) throws IOException {
      return null;
    }

    @Override
    public boolean ping(TaskAttemptID taskid) throws IOException {
      return false;
    }

    @Override
    public void done(TaskAttemptID taskid, boolean shouldBePromoted)
        throws IOException {

    }

    @Override
    public void fsError(TaskAttemptID taskId, String message)
        throws IOException {

    }

    @Override
    public void fatalError(TaskAttemptID taskId, String message)
        throws IOException {

    }

    @Override
    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus)
        throws IOException, InterruptedException {
      // does not need to be synchronized, because it is just an information.
      superStepCount = taskStatus.getSuperstepCount();
      return true;
    }

    @Override
    public int getAssignedPortNum(TaskAttemptID taskid) {
      return 0;
    }

  }

  public static class LocalSyncClient implements SyncClient {
    // note that this is static, because we will have multiple peers
    private static CyclicBarrier barrier;
    private int tasks;

    @Override
    public void init(Configuration conf, BSPJobID jobId, TaskAttemptID taskId)
        throws Exception {
      tasks = conf.getInt("bsp.peers.num", 1);

      synchronized (LocalSyncClient.class) {
        if (barrier == null) {
          barrier = new CyclicBarrier(tasks);
          LOG.info("Setting up a new barrier for " + tasks + " tasks!");
        }
      }
    }

    @Override
    public void enterBarrier(BSPJobID jobId, TaskAttemptID taskId,
        long superstep) throws Exception {
      barrier.await();
    }

    @Override
    public void leaveBarrier(BSPJobID jobId, TaskAttemptID taskId,
        long superstep) throws Exception {
      barrier.await();
    }

    @Override
    public void register(BSPJobID jobId, TaskAttemptID taskId,
        String hostAddress, long port) {
    }

    @Override
    public String[] getAllPeerNames(TaskAttemptID taskId) {
      return peerNames;
    }

    @Override
    public void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId,
        String hostAddress, long port) {

    }

    @Override
    public void stopServer() {

    }

    @Override
    public void close() throws Exception {

    }
  }
}
