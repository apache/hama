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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPMaster.State;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.zookeeper.KeeperException;

/**
 * A multithreaded local BSP runner that can be used for debugging and local
 * running BSP's.
 */
public class LocalBSPRunner implements JobSubmissionProtocol {
  public static final Log LOG = LogFactory.getLog(LocalBSPRunner.class);

  private static final String IDENTIFIER = "localrunner";
  private static String WORKING_DIR = "/user/hama/bsp/";
  protected static volatile ThreadPoolExecutor threadPool;
  protected static int threadPoolSize;
  protected static final LinkedList<Future<BSP>> futureList = new LinkedList<Future<BSP>>();
  protected static CyclicBarrier barrier;

  protected HashMap<String, LocalGroom> localGrooms = new HashMap<String, LocalGroom>();
  protected String jobFile;
  protected String jobName;

  protected JobStatus currentJobStatus;

  protected Configuration conf;
  protected FileSystem fs;

  public LocalBSPRunner(Configuration conf) throws IOException {
    super();
    this.conf = conf;
    String path = conf.get("bsp.local.dir");
    if (path != null && !path.isEmpty()) {
      WORKING_DIR = path;
    }

    threadPoolSize = conf.getInt("bsp.local.tasks.maximum", 20);
    threadPool = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(threadPoolSize);
    barrier = new CyclicBarrier(threadPoolSize);

    for (int i = 0; i < threadPoolSize; i++) {
      String name = IDENTIFIER + " " + i;
      localGrooms.put(name, new LocalGroom(name));
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

    if (fs == null)
      this.fs = FileSystem.get(conf);

    // add the resource to the current configuration, because add resouce in
    // HamaConfigurations constructor (ID,FILE) does not take local->HDFS
    // connections into account. This leads to not serializing the
    // configuration, which yields into failure.
    conf.addResource(fs.open(new Path(jobFile)));

    BSPJob job = new BSPJob(new HamaConfiguration(conf), jobID);
    job.setNumBspTask(threadPoolSize);

    this.jobName = job.getJobName();
    currentJobStatus = new JobStatus(jobID, System.getProperty("user.name"), 0,
        JobStatus.RUNNING);
    for (int i = 0; i < threadPoolSize; i++) {
      String name = IDENTIFIER + " " + i;
      LocalGroom localGroom = new LocalGroom(name);
      localGrooms.put(name, localGroom);
      futureList.add(threadPool.submit(new BSPRunner(conf, job, ReflectionUtils
          .newInstance(job.getBspClass(), conf), localGroom)));
    }
    new Thread(new ThreadObserver(currentJobStatus)).start();
    return currentJobStatus;
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    Map<String, GroomServerStatus> map = new HashMap<String, GroomServerStatus>();
    for (Entry<String, LocalGroom> entry : localGrooms.entrySet()) {
      map.put(entry.getKey(), new GroomServerStatus(entry.getKey(),
          new ArrayList<TaskStatus>(0), 0, 0, "", entry.getKey()));
    }
    return new ClusterStatus(map, threadPoolSize, threadPoolSize, State.RUNNING);
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    return new JobProfile(System.getProperty("user.name"), jobid, jobFile,
        jobName);
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    if (currentJobStatus == null) {
      currentJobStatus = new JobStatus(jobid, System.getProperty("user.name"),
          0L, JobStatus.RUNNING);
    }
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
  class BSPRunner implements Callable<BSP> {

    Configuration conf;
    BSPJob job;
    BSP bsp;
    LocalGroom groom;

    public BSPRunner(Configuration conf, BSPJob job, BSP bsp, LocalGroom groom) {
      super();
      this.conf = conf;
      this.job = job;
      this.bsp = bsp;
      this.groom = groom;
    }

    public void run() {
      bsp.setConf(conf);
      try {
        bsp.setup(groom);
        bsp.bsp(groom);
      } catch (Exception e) {
        LOG.error("Exception during BSP execution!", e);
      }
      bsp.cleanup(groom);
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
      for (Future<BSP> future : futureList) {
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

  class LocalGroom extends BSPPeer {
    private long superStepCount = 0;
    private final ConcurrentLinkedQueue<BSPMessage> localMessageQueue = new ConcurrentLinkedQueue<BSPMessage>();
    // outgoing queue
    private final Map<String, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>>();
    private final String peerName;

    public LocalGroom(String peerName) throws IOException {
      this.peerName = peerName;
    }

    @Override
    public void send(String peerName, BSPMessage msg) throws IOException {
      if (this.peerName.equals(peerName)) {
        put(msg);
      } else {
        // put this into a outgoing queue
        if (outgoingQueues.get(peerName) == null) {
          outgoingQueues.put(peerName, new ConcurrentLinkedQueue<BSPMessage>());
        }
        outgoingQueues.get(peerName).add(msg);
      }
    }

    @Override
    public void put(BSPMessage msg) throws IOException {
      localMessageQueue.add(msg);
    }

    @Override
    public BSPMessage getCurrentMessage() throws IOException {
      return localMessageQueue.poll();
    }

    @Override
    public int getNumCurrentMessages() {
      return localMessageQueue.size();
    }

    @Override
    public void sync() throws IOException, KeeperException,
        InterruptedException {
      // wait until all threads reach this barrier
      barrierSync();
      // send the messages
      for (Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry : outgoingQueues
          .entrySet()) {
        String peerName = entry.getKey();
        for (BSPMessage msg : entry.getValue())
          localGrooms.get(peerName).put(msg);
      }
      // clear the local outgoing queue
      outgoingQueues.clear();
      // sync again to avoid data inconsistency
      barrierSync();
      incrementSuperSteps();
    }

    private void barrierSync() throws InterruptedException {
      try {
        barrier.await();
      } catch (BrokenBarrierException e) {
        throw new InterruptedException("Barrier has been broken!" + e);
      }
    }

    private void incrementSuperSteps() {
      currentJobStatus.setprogress(superStepCount++);
      currentJobStatus.setSuperstepCount(currentJobStatus.progress());
    }

    @Override
    public long getSuperstepCount() {
      return superStepCount;
    }

    @Override
    public String getPeerName() {
      return peerName;
    }

    @Override
    public String[] getAllPeerNames() {
      return localGrooms.keySet().toArray(
          new String[localGrooms.keySet().size()]);
    }

    @Override
    public void clear() {
      localMessageQueue.clear();
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return 3;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void put(BSPMessageBundle messages) throws IOException {
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

  }
}
