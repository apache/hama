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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.GroomProtocol;
import org.apache.hama.monitor.Federator;
import org.apache.hama.monitor.Metric;
import org.apache.hama.monitor.MetricsRecord;
import org.apache.hama.monitor.Monitor;
import org.apache.hama.monitor.ZKCollector;
import org.apache.hama.monitor.Federator.Act;
import org.apache.hama.monitor.Federator.CollectorHandler;
import org.apache.zookeeper.ZooKeeper;

/**
 * A simple task scheduler. 
 */
class SimpleTaskScheduler extends TaskScheduler {

  private static final Log LOG = LogFactory.getLog(SimpleTaskScheduler.class);

  public static final String WAIT_QUEUE = "waitQueue";
  public static final String PROCESSING_QUEUE = "processingQueue";
  public static final String FINISHED_QUEUE = "finishedQueue";

  private final AtomicReference<QueueManager> queueManager = 
    new AtomicReference<QueueManager>();
  private AtomicBoolean initialized = new AtomicBoolean(false);
  private final JobListener jobListener;
  private final JobProcessor jobProcessor;
  private final AtomicReference<Federator> federator = 
    new AtomicReference<Federator>(); 
  private final ConcurrentMap<String, MetricsRecord> repository = 
    new ConcurrentHashMap<String, MetricsRecord>();
  private final ScheduledExecutorService scheduler;

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
        JobInProgress j = queue.removeJob();
        queueManager.get().addJob(PROCESSING_QUEUE, j);
        // schedule
        Collection<GroomServerStatus> glist = groomServerManager
            .groomServerStatusKeySet();
        schedule(j,
            (GroomServerStatus[]) glist.toArray(new GroomServerStatus[glist
                .size()]));
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
          .newScheduledThreadPool(1);// statuses.length + 5);

      ScheduledFuture<Boolean> jobScheduleResult = sched.schedule(
          new TaskWorker(statuses, numGroomServers, job), 0, SECONDS);

      Boolean jobResult = Boolean.FALSE;

      try {
        jobResult = jobScheduleResult.get();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        jobResult = Boolean.FALSE;
        LOG.error("Error submitting job", e);
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
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
  }

  private class TaskWorker implements Callable<Boolean> {
    private final Map<String, GroomServerStatus> groomStatuses;
    private final int groomNum;
    private final JobInProgress jip;

    TaskWorker(final GroomServerStatus[] stus, final int num,
        final JobInProgress jip) {
      this.groomStatuses = new HashMap<String, GroomServerStatus>(2 * num);
      for (GroomServerStatus status : stus) {
        this.groomStatuses.put(status.hostName, status);
      }
      this.groomNum = num;
      this.jip = jip;
      if (null == this.groomStatuses)
        throw new NullPointerException("Target groom server is not "
            + "specified.");
      if (-1 == this.groomNum)
        throw new IllegalArgumentException("Groom number is not specified.");
      if (null == this.jip)
        throw new NullPointerException("No job is specified.");
    }

    public Boolean call() {

      // Action to be sent for each task to the respective groom server.
      Map<GroomServerStatus, List<LaunchTaskAction>> actionMap = 
          new HashMap<GroomServerStatus, List<LaunchTaskAction>>(
              2 * this.groomStatuses.size());
      Set<Task> taskSet = new HashSet<Task>(2 * jip.tasks.length);
      Task t = null;
      int cnt = 0;
      while ((t = jip.obtainNewTask(this.groomStatuses)) != null) {
        taskSet.add(t);
        // Scheduled all tasks
        if (++cnt == this.jip.tasks.length) {
          break;
        }
      }

      // if all tasks could not be scheduled
      if (cnt != this.jip.tasks.length) {
        return Boolean.FALSE;
      }

      // assembly into actions
      Iterator<Task> taskIter = taskSet.iterator();
      while (taskIter.hasNext()) {
        Task task = taskIter.next();
        GroomServerStatus groomStatus = jip.getGroomStatusForTask(task);
        List<LaunchTaskAction> taskActions = actionMap.get(groomStatus);
        if (taskActions == null) {
          taskActions = new ArrayList<LaunchTaskAction>(
              groomStatus.getMaxTasks());
        }
        taskActions.add(new LaunchTaskAction(task));
        actionMap.put(groomStatus, taskActions);
      }

      Iterator<GroomServerStatus> groomIter = actionMap.keySet().iterator();
      while (jip.getStatus().getRunState() == JobStatus.RUNNING
          && groomIter.hasNext()) {

        GroomServerStatus groomStatus = groomIter.next();
        List<LaunchTaskAction> actionList = actionMap.get(groomStatus);

        GroomProtocol worker = groomServerManager.findGroomServer(groomStatus);
        try {
          // dispatch() to the groom server
          GroomServerAction[] actions = new GroomServerAction[actionList.size()];
          actionList.toArray(actions);
          Directive d1 = new DispatchTasksDirective(actions);
          worker.dispatch(d1);
        } catch (IOException ioe) {
          LOG.error(
              "Fail to dispatch tasks to GroomServer "
                  + groomStatus.getGroomName(), ioe);
        }

      }

      if (groomIter.hasNext()
          && jip.getStatus().getRunState() != JobStatus.RUNNING) {
        LOG.warn("Currently master only shcedules job in running state. "
            + "This may be refined in the future. JobId:" + jip.getJobID());
      }

      return Boolean.TRUE;
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
    public void run() {
      for(GroomServerStatus status: 
          groomServerManager.groomServerStatusKeySet()) {
        final String groom = status.getGroomName();
        final String jvmPath = Monitor.MONITOR_ROOT_PATH+groom+"/metrics/jvm";
        final Act act = 
          new Act(new ZKCollector(zk, "jvm", "Jvm metrics.", jvmPath), 
                  new CollectorHandler() {
            public void handle(Future future) {
              try {
                MetricsRecord record = (MetricsRecord)future.get();
                if(null != record) {
                  if(LOG.isDebugEnabled()) {
                    for(Metric metric: record.metrics()) {
                      LOG.debug("Metric name:"+metric.name()+" metric value:"+metric.value());
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
    if(initialized.get()) 
      throw new IllegalStateException(SimpleTaskScheduler.class.getSimpleName()+
      " is started.");
    this.queueManager.set(new QueueManager(getConf())); 
    this.federator.set(new Federator((HamaConfiguration)getConf()));
    this.queueManager.get().createFCFSQueue(WAIT_QUEUE);
    this.queueManager.get().createFCFSQueue(PROCESSING_QUEUE);
    this.queueManager.get().createFCFSQueue(FINISHED_QUEUE);
    groomServerManager.addJobInProgressListener(this.jobListener);
    this.initialized.set(true);
    if(null != getConf() && 
       getConf().getBoolean("bsp.federator.enabled", false)) {
      this.federator.get().start();
    }
    this.jobProcessor.start();
    if(null != getConf() && 
       getConf().getBoolean("bsp.federator.enabled", false)) {
      this.scheduler.scheduleAtFixedRate(new JvmCollector(federator.get(), 
      ((BSPMaster)groomServerManager).zk), 5, 5, SECONDS);
    }
  }

  @Override
  public void terminate() {
    this.initialized.set(false);
    if (null != this.jobListener)
      groomServerManager.removeJobInProgressListener(this.jobListener);
    this.jobProcessor.interrupt();
    this.federator.get().interrupt();
  }

  @Override
  public Collection<JobInProgress> getJobs(String queue) {
    return (queueManager.get().findQueue(queue)).jobs();
  }
}
