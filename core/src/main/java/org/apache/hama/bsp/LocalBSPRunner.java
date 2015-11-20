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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.BSPMaster.State;
import org.apache.hama.bsp.message.AbstractMessageManager;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.MessageManagerFactory;
import org.apache.hama.bsp.sync.BSPPeerSyncClient;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncEvent;
import org.apache.hama.bsp.sync.SyncEventListener;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.ipc.JobSubmissionProtocol;

/**
 * A multithreaded local BSP runner that can be used for debugging and local
 * running BSP's.
 */
public class LocalBSPRunner implements JobSubmissionProtocol {
  private static final Log LOG = LogFactory.getLog(LocalBSPRunner.class);

  private static final String IDENTIFIER = "localrunner";
  private static String WORKING_DIR = "/tmp/hama-bsp/";
  private volatile ThreadPoolExecutor threadPool;

  private String jobFile;
  private String jobName;

  private JobStatus currentJobStatus;

  private final Configuration conf;
  private FileSystem fs;

  private static volatile long superStepCount = 0L;
  private static String[] peerNames;
  private final Counters globalCounters = new Counters();

  // this is used for not-input driven job
  private final int maxTasks;
  // currently running tasks
  private final int totalTasks = 0;

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
        0L, JobStatus.RUNNING, globalCounters);

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

    threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numBspTask);
    @SuppressWarnings("rawtypes")
    ExecutorCompletionService<BSPPeerImpl> completionService = new ExecutorCompletionService<BSPPeerImpl>(
        threadPool);

    peerNames = new String[numBspTask];
    for (int i = 0; i < numBspTask; i++) {
      peerNames[i] = "local:" + i;
      completionService.submit(new BSPRunner(new HamaConfiguration(conf), job,
          i, splits));
      globalCounters.incrCounter(JobInProgress.JobCounter.LAUNCHED_TASKS, 1L);
    }

    new Thread(new ThreadObserver(numBspTask, completionService)).start();
    return currentJobStatus;
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    return new ClusterStatus(maxTasks, totalTasks, maxTasks, State.RUNNING);
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    return new JobProfile(System.getProperty("user.name"), jobid, jobFile,
        jobName);
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    currentJobStatus.setSuperstepCount(superStepCount);
    currentJobStatus.setProgress(superStepCount);
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
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    return false;
  }

  // this class will spawn a new thread and executes the BSP
  @SuppressWarnings({ "rawtypes" })
  static class BSPRunner implements Callable<BSPPeerImpl> {

    private final HamaConfiguration conf;
    private final BSPJob job;
    private final int id;
    private final BSP bsp;
    private final RawSplit[] splits;
    private BSPPeerImpl peer;

    public BSPRunner(HamaConfiguration conf, BSPJob job, int id,
        RawSplit[] splits) {
      super();
      this.conf = conf;
      this.job = job;
      this.id = id;
      this.splits = splits;

      // set the peer port to the id, to prevent collision
      conf.setInt(Constants.PEER_PORT, id);
      conf.set(Constants.PEER_HOST, "local");

      bsp = (BSP) ReflectionUtils.newInstance(
          job.getConfiguration().getClass("bsp.work.class", BSP.class),
          job.getConfiguration());

    }

    @SuppressWarnings("unchecked")
    public void run() throws Exception {

      String splitname = null;
      BytesWritable realBytes = null;
      if (splits != null && splits.length > id) {
        splitname = splits[id].getClassName();
        realBytes = splits[id].getBytes();
      }

      peer = new BSPPeerImpl(job, conf, new TaskAttemptID(new TaskID(
          job.getJobID(), id), id), new LocalUmbilical(), id, splitname,
          realBytes, new Counters());
      // Throw the first exception and log all the other exception.
      Exception firstException = null;
      try {
        bsp.setup(peer);
        bsp.bsp(peer);
      } catch (Exception e) {
        LOG.error("Exception during BSP execution!", e);
        firstException = e;
      } finally {
        try {
          bsp.cleanup(peer);
        } catch (Exception e) {
          LOG.error("Error cleaning up after bsp execution.", e);
          if (firstException == null)
            firstException = e;
        } finally {
          try {
            peer.clear();
            peer.close();
          } catch (Exception e) {
            LOG.error("Exception closing BSP peer,", e);
            if (firstException == null)
              firstException = e;
          } finally {
            if (firstException != null)
              throw firstException;
          }
        }

      }
    }

    @Override
    public BSPPeerImpl call() throws Exception {
      run();
      return peer;
    }
  }

  // this thread observes the status of the runners.
  @SuppressWarnings("rawtypes")
  class ThreadObserver implements Runnable {

    private final ExecutorCompletionService<BSPPeerImpl> completionService;
    private final int numTasks;

    public ThreadObserver(int numTasks,

    ExecutorCompletionService<BSPPeerImpl> completionService) {
      this.numTasks = numTasks;
      this.completionService = completionService;
    }

    @Override
    public void run() {
      boolean success = true;

      for (int i = 0; i < numTasks; i++) {
        try {
          Future<BSPPeerImpl> take = completionService.take();
          if (take != null) {
            currentJobStatus.getCounter().incrAllCounters(
                take.get().getCounters());
          }
        } catch (Exception e) {
          LOG.error("Exception during BSP execution!", e);
          success = false;
          break;
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

  public static class LocalMessageManager<M extends Writable> extends
      AbstractMessageManager<M> {

    @SuppressWarnings("rawtypes")
    private static final ConcurrentHashMap<InetSocketAddress, LocalMessageManager> MANAGER_MAP = new ConcurrentHashMap<InetSocketAddress, LocalBSPRunner.LocalMessageManager>();
    private InetSocketAddress selfAddress;

    @Override
    public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, M> peer,
        HamaConfiguration conf, InetSocketAddress peerAddress) {
      super.init(attemptId, peer, conf, peerAddress);
      MANAGER_MAP.put(peerAddress, this);
      selfAddress = peerAddress;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
        throws IOException {
      // peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGE_BYTES_TRANSFERED,
      // bundle.getLength());

      MANAGER_MAP.get(addr).localQueueForNextIteration.addBundle(bundle);
      peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED,
          bundle.size());
    }

    @Override
    public InetSocketAddress getListenerAddress() {
      return selfAddress;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void transfer(InetSocketAddress addr, M msg) throws IOException {
      MANAGER_MAP.get(addr).localQueueForNextIteration.add(msg);
      peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED, 1);
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
    public void done(TaskAttemptID taskid) throws IOException {

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
      return true;
    }

    @Override
    public int getAssignedPortNum(TaskAttemptID taskid) {
      // TODO Auto-generated method stub
      return 0;
    }
  }

  public static class LocalSyncClient extends BSPPeerSyncClient {
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
        long superstep) throws SyncException {
      try {
        barrier.await();
      } catch (Exception e) {
        throw new SyncException(e.toString());
      }
    }

    @Override
    public void leaveBarrier(BSPJobID jobId, TaskAttemptID taskId,
        long superstep) throws SyncException {
      try {
        barrier.await();
      } catch (Exception e) {
        throw new SyncException(e.toString());
      }
      if (superstep > superStepCount)
        superStepCount = superstep;
    }

    @Override
    public void register(BSPJobID jobId, TaskAttemptID taskId,
        String hostAddress, long port) {
    }

    @Override
    public void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId,
        String hostAddress, long port) {

    }

    @Override
    public void stopServer() {

    }

    @Override
    public void close() {
      barrier = null;
    }

    @Override
    public String constructKey(BSPJobID jobId, String... args) {
      return null;
    }

    @Override
    public boolean storeInformation(String key, Writable value,
        boolean permanent, SyncEventListener listener) {
      return false;
    }

    @Override
    public boolean getInformation(String key, Writable valueHolder) {
      return false;
    }

    @Override
    public boolean addKey(String key, boolean permanent,
        SyncEventListener listener) {
      return false;
    }

    @Override
    public boolean hasKey(String key) {
      return false;
    }

    @Override
    public String[] getChildKeySet(String key, SyncEventListener listener) {
      return null;
    }

    @Override
    public boolean registerListener(String key, SyncEvent event,
        SyncEventListener listener) {
      return false;
    }

    @Override
    public boolean remove(String key, SyncEventListener listener) {
      return false;
    }

    @Override
    public String[] getAllPeerNames(BSPJobID jobID) {
      return peerNames;
    }
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(BSPJobID id,
      int startFrom, int i) {
    return TaskCompletionEvent.EMPTY_ARRAY;
  }
}
