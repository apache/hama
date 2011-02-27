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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.ipc.WorkerProtocol;

/**
 * A simple task scheduler. 
 */
class SimpleTaskScheduler extends TaskScheduler {

  private static final Log LOG = LogFactory.getLog(SimpleTaskScheduler.class);

  public static final String WAIT_QUEUE = "waitQueue";
  public static final String PROCESSING_QUEUE = "processingQueue";
  public static final String FINISHED_QUEUE = "finishedQueue";

  private QueueManager queueManager;
  private volatile boolean initialized;
  private JobListener jobListener;
  private JobProcessor jobProcessor;

  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) throws IOException {
      queueManager.initJob(job); // init task
      queueManager.addJob(WAIT_QUEUE, job);
    }

    @Override
    public void jobRemoved(JobInProgress job) throws IOException {
      // queueManager.removeJob(WAIT_QUEUE, job);
      queueManager.moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
    }
  }

  private class JobProcessor extends Thread implements Schedulable {
    JobProcessor() {
      super("JobProcess");
    }

    /**
     * Main logic scheduling task to GroomServer(s). Also, it will move
     * JobInProgress from Wait Queue to Processing Queue.
     */
    public void run() {
      if (false == initialized) {
        throw new IllegalStateException("SimpleTaskScheduler initialization"
            + " is not yet finished!");
      }
      while (initialized) {
        Queue<JobInProgress> queue = queueManager.findQueue(WAIT_QUEUE);
        if (null == queue) {
          LOG.error(WAIT_QUEUE + " does not exist.");
          throw new NullPointerException(WAIT_QUEUE + " does not exist.");
        }
        // move a job from the wait queue to the processing queue
        JobInProgress j = queue.removeJob();
        queueManager.addJob(PROCESSING_QUEUE, j);
        // schedule
        Collection<GroomServerStatus> glist = groomServerManager
            .groomServerStatusKeySet();
        schedule(j, (GroomServerStatus[]) glist
            .toArray(new GroomServerStatus[glist.size()]));
      }
    }

    /**
     * Schedule job to designated GroomServer(s) immediately.
     * 
     * @param Targeted GroomServer(s).
     * @param Job to be scheduled.
     */
    @Override
    public void schedule(JobInProgress job, GroomServerStatus... statuses) {
      ClusterStatus clusterStatus = groomServerManager.getClusterStatus(false);
      final int numGroomServers = clusterStatus.getGroomServers();
      final ScheduledExecutorService sched = Executors
          .newScheduledThreadPool(statuses.length + 5);
      for (GroomServerStatus status : statuses) {
        sched
            .schedule(new TaskWorker(status, numGroomServers, job), 0, SECONDS);
      }// for
    }
  }

  private class TaskWorker implements Runnable {
    private final GroomServerStatus stus;
    private final int groomNum;
    private final JobInProgress jip;

    TaskWorker(final GroomServerStatus stus, final int num,
        final JobInProgress jip) {
      this.stus = stus;
      this.groomNum = num;
      this.jip = jip;
      if (null == this.stus)
        throw new NullPointerException("Target groom server is not "
            + "specified.");
      if (-1 == this.groomNum)
        throw new IllegalArgumentException("Groom number is not specified.");
      if (null == this.jip)
        throw new NullPointerException("No job is specified.");
    }

    public void run() {
      // obtain tasks
      Task t = jip.obtainNewTask(this.stus, groomNum);
      // assembly into actions
      // List<Task> tasks = new ArrayList<Task>();
      if (jip.getStatus().getRunState() == JobStatus.RUNNING) {
        WorkerProtocol worker = groomServerManager.findGroomServer(this.stus);
        try {
          // dispatch() to the groom server
          Directive d1 = new Directive(groomServerManager
              .currentGroomServerPeers(),
              new GroomServerAction[] { new LaunchTaskAction(t) });
          worker.dispatch(d1);
        } catch (IOException ioe) {
          LOG.error("Fail to dispatch tasks to GroomServer "
              + this.stus.getGroomName(), ioe);
        }
      } else {
        LOG.warn("Currently master only shcedules job in running state. "
            + "This may be refined in the future. JobId:" + jip.getJobID());
      }
    }
  }

  public SimpleTaskScheduler() {
    this.jobListener = new JobListener();
    this.jobProcessor = new JobProcessor();
  }

  @Override
  public void start() {
    this.queueManager = new QueueManager(getConf()); // TODO: need factory?
    this.queueManager.createFCFSQueue(WAIT_QUEUE);
    this.queueManager.createFCFSQueue(PROCESSING_QUEUE);
    this.queueManager.createFCFSQueue(FINISHED_QUEUE);
    groomServerManager.addJobInProgressListener(this.jobListener);
    this.initialized = true;
    this.jobProcessor.start();
  }

  @Override
  public void terminate() {
    this.initialized = false;
    if (null != this.jobListener)
      groomServerManager.removeJobInProgressListener(this.jobListener);
  }

  @Override
  public Collection<JobInProgress> getJobs(String queue) {
    return (queueManager.findQueue(queue)).jobs();
    // return jobQueue;
  }
}
