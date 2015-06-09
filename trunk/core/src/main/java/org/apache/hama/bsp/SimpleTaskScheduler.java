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
import static org.apache.hama.monitor.fd.NodeStatus.Dead;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.ZKSyncBSPMasterClient;
import org.apache.hama.monitor.Federator;
import org.apache.hama.monitor.Federator.Act;
import org.apache.hama.monitor.Federator.CollectorHandler;
import org.apache.hama.monitor.Metric;
import org.apache.hama.monitor.MetricsRecord;
import org.apache.hama.monitor.Monitor;
import org.apache.hama.monitor.ZKCollector;
import org.apache.hama.monitor.fd.NodeEventListener;
import org.apache.hama.monitor.fd.NodeStatus;
import org.apache.zookeeper.ZooKeeper;


/**
 * A simple task scheduler with FCFS processing queue.
 */
class SimpleTaskScheduler extends TaskScheduler {

  private static final Log LOG = LogFactory.getLog(SimpleTaskScheduler.class);

  public static final String WAIT_QUEUE = "waitQueue";
  public static final String PROCESSING_QUEUE = "processingQueue";
  public static final String FINISHED_QUEUE = "finishedQueue";

  private final AtomicReference<QueueManager> queueManager = new AtomicReference<QueueManager>();
  private AtomicBoolean initialized = new AtomicBoolean(false);
  private final JobListener jobListener;
  private final JobProcessor jobProcessor;
  private TaskWorkerManager taskWorkerManager;
  private final AtomicReference<Federator> federator = new AtomicReference<Federator>();
  /** <String, MetricsRecord> maps to <groom server, metrics record> */
  private final ConcurrentMap<String, MetricsRecord> repository = new ConcurrentHashMap<String, MetricsRecord>();
  private final ScheduledExecutorService scheduler;

  final class NodeWatcher implements NodeEventListener {
    final GroomServerManager groomManager;
    final TaskScheduler _sched;

    NodeWatcher(GroomServerManager groomManager, TaskScheduler _sched) {
      this.groomManager = groomManager;
      this._sched = _sched;
    }

    @Override
    public NodeStatus[] interest() {
      return new NodeStatus[] { Dead };
    }

    @Override
    public String name() {
      return SimpleTaskScheduler.class.getSimpleName() + "'s "
          + NodeWatcher.class.getSimpleName();
    }

    /**
     * Trigger to reschedule all tasks running on a failed GroomSever. Note that
     * this method is trigger only when a groom server fails (detected by
     * failure detector). BSPMaster has no way to send kill directive to the
     * groom server because a failed GroomServer can't respond.
     * 
     * @param status of the groom server, reported by failure detector.
     * @param host is the groom server on which tasks run.
     */
    @Override
    public void notify(NodeStatus status, String host) {
      // TODO:
    }
  }

  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) throws IOException {
      queueManager.get().initJob(job); // init task
      queueManager.get().addJob(WAIT_QUEUE, job);
    }

    @Override
    public void jobRemoved(JobInProgress job) throws IOException {
      queueManager.get().moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
    }

    @Override
    public void recoverTaskInJob(JobInProgress job) throws IOException {
      queueManager.get().addJob(WAIT_QUEUE, job);
    }

  }

  private class JobProcessor extends Thread implements Schedulable {

    final ExecutorService sched;

    JobProcessor() {
      super("JobProcessor");
      this.sched = Executors.newCachedThreadPool();
    }

    /**
     * Main logic of scheduling tasks to GroomServer(s). Also, it will move
     * JobInProgress from Wait Queue to Processing Queue.
     */
    @Override
    public void run() {
      if (!initialized.get()) {
        throw new IllegalStateException("SimpleTaskScheduler initialization"
            + " is not yet finished!");
      }
      while (initialized.get()) {
        Queue<JobInProgress> queue = queueManager.get().findQueue(WAIT_QUEUE);
        if (null == queue) {
          LOG.error(WAIT_QUEUE + " does not exist.");
          throw new NullPointerException(WAIT_QUEUE + " does not exist.");
        }
        // move a job from the wait queue to the processing queue
        JobInProgress job = queue.removeJob();
        queueManager.get().addJob(PROCESSING_QUEUE, job);
        // schedule

        schedule(job);
      }
    }

    /**
     * Schedule job to designated GroomServer(s) immediately.
     * 
     * @param Targeted GroomServer(s).
     * @param Job to be scheduled.
     */
    @Override
    public void schedule(JobInProgress job) {
      Future<Boolean> jobScheduleResult = sched.submit(taskWorkerManager.spawnWorker(job));

      Boolean jobResult = Boolean.FALSE;

      try {
        jobResult = jobScheduleResult.get();
      } catch (InterruptedException e) {
        jobResult = Boolean.FALSE;
        LOG.error("Error submitting job", e);
      } catch (ExecutionException e) {
        jobResult = Boolean.FALSE;
        LOG.error("Error submitting job", e);
      }
      if (Boolean.FALSE.equals(jobResult)) {
        LOG.error(new StringBuffer(512).append("Scheduling of job ")
            .append(job.getJobName())
            .append(" could not be done successfully. Killing it!").toString());
        job.kill();
      }
    }

    @Override
    public void interrupt() {
      super.interrupt();
      this.sched.shutdown();
    }
  }

  


  /**
   * Periodically collect metrics info.
   */
  private class JvmCollector implements Runnable {
    final Federator federator;
    final ZooKeeper zk;

    JvmCollector(final Federator federator, final ZooKeeper zk) {
      this.federator = federator;
      this.zk = zk;
    }

    @Override
    public void run() {
      for (GroomServerStatus status : groomServerManager.get()
          .groomServerStatusKeySet()) {
        final String groom = status.getGroomName();
        final String jvmPath = Monitor.MONITOR_ROOT_PATH + groom
            + "/metrics/jvm";
        final Act act = new Act(new ZKCollector(zk, "jvm", "Jvm metrics.",
            jvmPath), new CollectorHandler() {
          @Override
          public void handle(@SuppressWarnings("rawtypes") Future future) {
            try {
              MetricsRecord record = (MetricsRecord) future.get();
              if (null != record) {
                if (LOG.isDebugEnabled()) {
                  for (@SuppressWarnings("rawtypes")
                  Metric metric : record.metrics()) {
                    LOG.debug("Metric name:" + metric.name() + " metric value:"
                        + metric.value());
                  }
                }
                repository.put(groom, record);
              }
            } catch (InterruptedException ie) {
              LOG.warn(ie);
              Thread.currentThread().interrupt();
            } catch (ExecutionException ee) {
              LOG.warn(ee.getCause());
            }
          }
        });
        this.federator.register(act);
      }
    }
  }

  public SimpleTaskScheduler() {
    this.jobListener = new JobListener();
    this.jobProcessor = new JobProcessor();
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void start() {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException(SimpleTaskScheduler.class.getSimpleName()
          + " is started.");
    }
    this.queueManager.set(new QueueManager(getConf()));
    this.federator.set(new Federator((HamaConfiguration) getConf()));
    this.queueManager.get().createFCFSQueue(WAIT_QUEUE);
    this.queueManager.get().createFCFSQueue(PROCESSING_QUEUE);
    this.queueManager.get().createFCFSQueue(FINISHED_QUEUE);
    groomServerManager.get().addJobInProgressListener(this.jobListener);

    // Create and initialize the task worker manager
    Class<? extends TaskWorkerManager> taskWorkerClass = conf.getClass(
    		Constants.TASK_EXECUTOR_CLASS, SimpleTaskWorkerManager.class,
        TaskWorkerManager.class);
    this.taskWorkerManager = ReflectionUtils.newInstance(taskWorkerClass, conf);
    this.taskWorkerManager.init(groomServerManager, conf);

    
    if (null != getConf()
        && getConf().getBoolean("bsp.federator.enabled", false)) {
      this.federator.get().start();
    }
    this.jobProcessor.start();
    if (null != getConf()
        && getConf().getBoolean("bsp.federator.enabled", false)) {
      this.scheduler.scheduleAtFixedRate(
          new JvmCollector(federator.get(),
              ((ZKSyncBSPMasterClient) ((BSPMaster) groomServerManager.get())
                  .getSyncClient()).getZK()), 5, 5, SECONDS);
    }

    if (null != monitorManager.get()) {
      if (null != monitorManager.get().supervisor()) {
        monitorManager.get().supervisor()
            .register(new NodeWatcher(groomServerManager.get(), this));
      }
    }
  }

  @Override
  public void terminate() {
    this.initialized.set(false);
    if (null != this.jobListener)
      groomServerManager.get().removeJobInProgressListener(this.jobListener);
    this.jobProcessor.interrupt();
    this.federator.get().interrupt();
  }

  @Override
  public Collection<JobInProgress> getJobs(String queue) {
    return (queueManager.get().findQueue(queue)).jobs();
  }

  @Override
  public JobInProgress findJobById(BSPJobID id) {
    for (JobInProgress job : getJobs(PROCESSING_QUEUE)) {
      if (job.getJobID().equals(id)) {
        return job;
      }
    }
    return null;
  }

}
