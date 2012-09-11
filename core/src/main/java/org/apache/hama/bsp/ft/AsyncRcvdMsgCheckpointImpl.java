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
package org.apache.hama.bsp.ft;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.GroomServerAction;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.JobInProgress;
import org.apache.hama.bsp.RecoverTaskAction;
import org.apache.hama.bsp.Task;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.bsp.TaskInProgress;
import org.apache.hama.bsp.TaskStatus;
import org.apache.hama.bsp.message.MessageEventListener;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.sync.MasterSyncClient;
import org.apache.hama.bsp.sync.PeerSyncClient;
import org.apache.hama.bsp.taskallocation.BSPResource;
import org.apache.hama.bsp.taskallocation.TaskAllocationStrategy;

/**
 * <code>AsyncRcvdMsgCheckpointImpl</code> Checkpoint service defines the fault
 * tolerance strategy by checkpointing of messages sent across peers. On
 * failure, all the tasks are restarted from the last superstep for which all
 * the peers successfully checkpointed the messages.
 * 
 */
public class AsyncRcvdMsgCheckpointImpl<M extends Writable> implements
    BSPFaultTolerantService<M> {

  private static final Log LOG = LogFactory
      .getLog(AsyncRcvdMsgCheckpointImpl.class);

  /**
   * It is responsible to find the smallest superstep for which the
   * checkpointing is done and then restart all the peers from that superstep.
   */
  private static class CheckpointMasterService implements
      FaultTolerantMasterService {

    private Configuration conf;
    private TaskInProgress tasks[];
    private BSPJobID jobId;
    private int maxTaskAttempts;
    private int currentAttemptId;
    private MasterSyncClient masterSyncClient;
    private TaskAllocationStrategy allocationStrategy;

    /**
     * Initializes the fault tolerance service at BSPMasters
     * 
     * @param jobId The identifier of the job.
     * @param maxTaskAttempts Number of attempts allowed for recovering from
     *          failure.
     * @param tasks The list of tasks in the job.
     * @param conf The job configuration object.
     * @param masterClient The synchronization client used by BSPMaster.
     * @param allocationStrategy The task allocation strategy of the job.
     */
    public void initialize(BSPJobID jobId, int maxTaskAttempts,
        TaskInProgress[] tasks, Configuration conf,
        MasterSyncClient masterClient, TaskAllocationStrategy allocationStrategy) {
      this.tasks = tasks;
      this.jobId = jobId;
      this.conf = conf;
      this.maxTaskAttempts = maxTaskAttempts;
      this.currentAttemptId = 0;
      this.masterSyncClient = masterClient;
      this.allocationStrategy = allocationStrategy;
    }

    @Override
    public boolean isRecoveryPossible(TaskInProgress tip) {
      return currentAttemptId < maxTaskAttempts;
    }

    @Override
    public boolean isAlreadyRecovered(TaskInProgress tip) {
      return currentAttemptId < tip.getCurrentTaskAttemptId().getId();
    }

    @Override
    public void recoverTasks(JobInProgress jip,
        Map<String, GroomServerStatus> groomStatuses,
        TaskInProgress[] failedTasksInProgress,
        TaskInProgress[] allTasksInProgress,
        Map<GroomServerStatus, Integer> taskCountInGroomMap,
        Map<GroomServerStatus, List<GroomServerAction>> actionMap)
        throws IOException {

      Map<TaskID, TaskInProgress> recoverySet = new HashMap<TaskID, TaskInProgress>(
          2 * failedTasksInProgress.length);
      for (int i = 0; i < failedTasksInProgress.length; ++i) {
        recoverySet.put(failedTasksInProgress[i].getTaskId(),
            failedTasksInProgress[i]);
      }

      long lowestSuperstepNumber = Long.MAX_VALUE;

      String[] taskProgress = this.masterSyncClient.getChildKeySet(
          this.masterSyncClient.constructKey(jobId, "checkpoint"), null);

      if (LOG.isDebugEnabled()) {
        StringBuffer list = new StringBuffer(25 * taskProgress.length);
        list.append("got child key set").append(taskProgress.length)
            .append("/").append(tasks.length).append(" ");
        for (String entry : taskProgress) {
          list.append(entry).append(",");
        }
        LOG.debug(list);
      }

      if (taskProgress.length == this.tasks.length) {
        for (int i = 0; i < taskProgress.length; ++i) {
          ArrayWritable progressInformation = new ArrayWritable(
              LongWritable.class);
          boolean result = this.masterSyncClient.getInformation(
              this.masterSyncClient.constructKey(jobId, "checkpoint",
                  taskProgress[i]), progressInformation);

          if (!result) {
            lowestSuperstepNumber = -1L;
            break;
          }

          Writable[] progressArr = progressInformation.get();
          LongWritable superstepProgress = (LongWritable) progressArr[0];

          if (superstepProgress != null) {
            if (superstepProgress.get() < lowestSuperstepNumber) {
              lowestSuperstepNumber = superstepProgress.get();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Got superstep number " + lowestSuperstepNumber
                    + " from " + taskProgress[i]);
              }
            }
          }
        }
        clearClientForSuperstep(lowestSuperstepNumber);
        restartJob(lowestSuperstepNumber, groomStatuses, recoverySet,
            allTasksInProgress, taskCountInGroomMap, actionMap);

      } else {
        restartJob(-1, groomStatuses, recoverySet, allTasksInProgress,
            taskCountInGroomMap, actionMap);
      }

      ++currentAttemptId;
    }

    private void clearClientForSuperstep(long superstep) {
      this.masterSyncClient.remove(
          masterSyncClient.constructKey(jobId, "sync"), null);
    }

    private void populateAction(Task task, long superstep,
        GroomServerStatus groomStatus,
        Map<GroomServerStatus, List<GroomServerAction>> actionMap) {
      List<GroomServerAction> list = actionMap.get(groomStatus);
      if (!actionMap.containsKey(groomStatus)) {
        list = new ArrayList<GroomServerAction>();
        actionMap.put(groomStatus, list);
      }
      list.add(new RecoverTaskAction(task, superstep));

    }

    private void restartTask(TaskInProgress tip, long superstep,
        Map<String, GroomServerStatus> groomStatuses,
        Map<GroomServerStatus, List<GroomServerAction>> actionMap) {
      GroomServerStatus serverStatus = tip.getGroomServerStatus();
      Task task = tip.constructTask(serverStatus);
      populateAction(task, superstep, serverStatus, actionMap);

    }

    private void restartJob(long superstep,
        Map<String, GroomServerStatus> groomStatuses,
        Map<TaskID, TaskInProgress> recoveryMap, TaskInProgress[] allTasks,
        Map<GroomServerStatus, Integer> taskCountInGroomMap,
        Map<GroomServerStatus, List<GroomServerAction>> actionMap)
        throws IOException {
      String path = conf.get("bsp.checkpoint.prefix_path", "/checkpoint/");

      if (superstep >= 0) {
        FileSystem fileSystem = FileSystem.get(conf);
        for (int i = 0; i < allTasks.length; ++i) {
          String[] hosts = null;
          if (recoveryMap.containsKey(allTasks[i].getTaskId())) {

            // Update task count in map.
            // TODO: This should be a responsibility of GroomServerStatus
            Integer count = taskCountInGroomMap.get(allTasks[i]
                .getGroomServerStatus());
            if (count != null) {
              count = count.intValue() - 1;
              taskCountInGroomMap
                  .put(allTasks[i].getGroomServerStatus(), count);
            }

            StringBuffer ckptPath = new StringBuffer(path);
            ckptPath.append(this.jobId.toString());
            ckptPath.append("/").append(superstep).append("/")
                .append(allTasks[i].getTaskId().getId());
            Path checkpointPath = new Path(ckptPath.toString());
            if (fileSystem.exists(checkpointPath)) {
              FileStatus fileStatus = fileSystem.getFileStatus(checkpointPath);
              BlockLocation[] blocks = fileSystem.getFileBlockLocations(
                  fileStatus, 0, fileStatus.getLen());
              hosts = blocks[0].getHosts();
            } else {
              hosts = new String[groomStatuses.keySet().size()];
              groomStatuses.keySet().toArray(hosts);
            }
            GroomServerStatus serverStatus = this.allocationStrategy
                .getGroomToAllocate(groomStatuses, hosts, taskCountInGroomMap,
                    new BSPResource[0], allTasks[i]);
            Task task = allTasks[i].constructTask(serverStatus);
            populateAction(task, superstep, serverStatus, actionMap);

          } else {
            restartTask(allTasks[i], superstep, groomStatuses, actionMap);
          }
        }
      } else {
        // Start the task from the beginning.
        for (int i = 0; i < allTasks.length; ++i) {
          if (recoveryMap.containsKey(allTasks[i].getTaskId())) {
            this.allocationStrategy.getGroomToAllocate(groomStatuses,
                this.allocationStrategy.selectGrooms(groomStatuses,
                    taskCountInGroomMap, new BSPResource[0], allTasks[i]),
                taskCountInGroomMap, new BSPResource[0], allTasks[i]);
          } else {
            restartTask(allTasks[i], superstep, groomStatuses, actionMap);
          }
        }
      }
    }

  }// end of CheckpointMasterService

  @Override
  public FaultTolerantPeerService<M> constructPeerFaultTolerance(BSPJob job,
      @SuppressWarnings("rawtypes")
      BSPPeer bspPeer, PeerSyncClient syncClient,
      InetSocketAddress peerAddress, TaskAttemptID taskAttemptId,
      long superstep, Configuration conf, MessageManager<M> messenger)
      throws Exception {
    CheckpointPeerService<M> service = new CheckpointPeerService<M>();
    service.initialize(job, bspPeer, syncClient, peerAddress, taskAttemptId,
        superstep, conf, messenger);
    return service;
  }

  @Override
  public FaultTolerantMasterService constructMasterFaultTolerance(
      BSPJobID jobId, int maxTaskAttempts, TaskInProgress[] tasks,
      Configuration conf, MasterSyncClient masterClient,
      TaskAllocationStrategy allocationStrategy) throws Exception {
    CheckpointMasterService service = new CheckpointMasterService();
    service.initialize(jobId, maxTaskAttempts, tasks, conf, masterClient,
        allocationStrategy);
    return service;
  }

  /**
   * Initializes the peer fault tolerance by checkpointing service. For
   * recovery, on peer initialization, it reads all the checkpointed messages to
   * recover the state of the peer. During normal working, it checkpoints all
   * the messages it received in the previous superstep. It also stores the
   * superstep progress in the global synchronization area.
   * 
   */
  public static class CheckpointPeerService<M extends Writable> implements
      FaultTolerantPeerService<M>, MessageEventListener<M> {

    private BSPJob job;
    @SuppressWarnings("rawtypes")
    private BSPPeer peer;
    private PeerSyncClient syncClient;
    private long superstep;
    private Configuration conf;
    private MessageManager<M> messenger;
    private FileSystem fs;
    private int checkPointInterval;
    volatile private long lastCheckPointStep;
    volatile private boolean checkpointState;
    volatile private FSDataOutputStream checkpointStream;
    volatile private long checkpointMessageCount;

    public void initialize(BSPJob job, @SuppressWarnings("rawtypes")
    BSPPeer bspPeer, PeerSyncClient syncClient, InetSocketAddress peerAddress,
        TaskAttemptID taskAttemptId, long superstep, Configuration conf,
        MessageManager<M> messenger) throws IOException {

      this.job = job;
      this.peer = bspPeer;
      this.syncClient = syncClient;
      this.superstep = superstep;
      this.conf = conf;
      this.messenger = messenger;
      this.fs = FileSystem.get(conf);
      this.checkPointInterval = conf.getInt(Constants.CHECKPOINT_INTERVAL,
          Constants.DEFAULT_CHECKPOINT_INTERVAL);
      this.checkPointInterval = conf.getInt(Constants.CHECKPOINT_INTERVAL,
          Constants.DEFAULT_CHECKPOINT_INTERVAL);

      this.checkpointState = conf.getBoolean(Constants.CHECKPOINT_ENABLED,
          false);

      if (superstep > 0) {
        this.lastCheckPointStep = this.superstep;
      } else {
        this.lastCheckPointStep = 1;
      }
      this.checkpointMessageCount = 0L;
    }

    private String checkpointPath(long step) {
      String backup = conf.get("bsp.checkpoint.prefix_path", "checkpoint/");
      String ckptPath = backup + job.getJobID().toString() + "/" + (step) + "/"
          + peer.getPeerIndex();
      if (LOG.isDebugEnabled())
        LOG.debug("Received Messages are to be saved to " + ckptPath);
      return ckptPath;
    }

    @Override
    public TaskStatus.State onPeerInitialized(TaskStatus.State state)
        throws Exception {
      if (this.superstep >= 0 && state.equals(TaskStatus.State.RECOVERING)) {
        ArrayWritable progressArr = new ArrayWritable(LongWritable.class);
        boolean result = this.syncClient.getInformation(
            this.syncClient.constructKey(job.getJobID(), "checkpoint",
                String.valueOf(peer.getPeerIndex())), progressArr);

        if (!result) {
          throw new IOException("No data found to restore peer state.");
        }

        Writable[] progressInfo = progressArr.get();
        long superstepProgress = ((LongWritable) progressInfo[0]).get();
        long numMessages = ((LongWritable) progressInfo[1]).get();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Got sstep =" + superstepProgress + " numMessages = "
              + numMessages + " this.superstep = " + this.superstep);
        }

        if (numMessages > 0) {
          Path path = new Path(checkpointPath(superstepProgress));
          FSDataInputStream in = this.fs.open(path);
          BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
          try {
            for (int i = 0; i < numMessages; ++i) {
              String className = in.readUTF();
              @SuppressWarnings("unchecked")
              M message = (M) ReflectionUtils.newInstance(
                  Class.forName(className), conf);
              message.readFields(in);
              bundle.addMessage(message);
            }
            messenger.loopBackMessages(bundle);
          } catch (EOFException e) {
            LOG.error("Error recovering from checkpointing", e);
            throw new IOException(e);
          } finally {
            this.fs.close();
          }
        }
      }
      this.messenger.registerListener(this);
      return TaskStatus.State.RUNNING;

    }

    public final boolean isReadyToCheckpoint() {

      checkPointInterval = conf.getInt(Constants.CHECKPOINT_INTERVAL, 1);
      LOG.info(new StringBuffer(1000).append("Enabled = ")
          .append(conf.getBoolean(Constants.CHECKPOINT_ENABLED, false))
          .append(" checkPointInterval = ").append(checkPointInterval)
          .append(" lastCheckPointStep = ").append(lastCheckPointStep)
          .append(" getSuperstepCount() = ").append(peer.getSuperstepCount())
          .toString());
      if (LOG.isDebugEnabled())
        LOG.debug(new StringBuffer(1000).append("Enabled = ")
            .append(conf.getBoolean(Constants.CHECKPOINT_ENABLED, false))
            .append(" checkPointInterval = ").append(checkPointInterval)
            .append(" lastCheckPointStep = ").append(lastCheckPointStep)
            .append(" getSuperstepCount() = ").append(peer.getSuperstepCount())
            .toString());

      return (conf.getBoolean(Constants.CHECKPOINT_ENABLED, false)
          && (checkPointInterval != 0) && (((int) ((peer.getSuperstepCount() + 1) - lastCheckPointStep)) >= checkPointInterval));

    }

    @Override
    public void beforeBarrier() throws Exception {
    }

    @Override
    public void duringBarrier() throws Exception {
    }

    @Override
    public void afterBarrier() throws Exception {

      synchronized (this) {
        if (checkpointState) {

          if (checkpointStream != null) {
            this.checkpointStream.close();
            this.checkpointStream = null;
          }

          lastCheckPointStep = peer.getSuperstepCount();

          ArrayWritable writableArray = new ArrayWritable(LongWritable.class);
          Writable[] writeArr = new Writable[2];
          writeArr[0] = new LongWritable(lastCheckPointStep);
          writeArr[1] = new LongWritable(checkpointMessageCount);
          writableArray.set(writeArr);

          if (LOG.isDebugEnabled()) {
            LOG.debug("Storing lastCheckPointStep = " + lastCheckPointStep
                + " checkpointMessageCount = " + checkpointMessageCount
                + " for peer = " + String.valueOf(peer.getPeerIndex()));
          }

          this.syncClient.storeInformation(this.syncClient.constructKey(
              this.job.getJobID(), "checkpoint",
              String.valueOf(peer.getPeerIndex())), writableArray, true, null);
        }
        checkpointState = isReadyToCheckpoint();
        checkpointMessageCount = 0;
      }

      LOG.info("checkpointNext = " + checkpointState
          + " checkpointMessageCount = " + checkpointMessageCount);
    }

    @Override
    public void onInitialized() {

    }

    @Override
    public void onMessageSent(String peerName, M message) {
    }

    @Override
    public void onMessageReceived(M message) {
      String checkpointedPath = null;

      if (message == null) {
        LOG.error("Message M is found to be null");
      }

      synchronized (this) {
        if (checkpointState) {
          if (this.checkpointStream == null) {
            checkpointedPath = checkpointPath(peer.getSuperstepCount() + 1);
            try {
              LOG.info("Creating path " + checkpointedPath);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Creating path " + checkpointedPath);
              }
              checkpointStream = this.fs.create(new Path(checkpointedPath));
            } catch (IOException ioe) {
              LOG.error("Fail checkpointing messages to " + checkpointedPath,
                  ioe);
              throw new RuntimeException("Failed opening HDFS file "
                  + checkpointedPath, ioe);
            }
          }
          try {
            ++checkpointMessageCount;
            checkpointStream.writeUTF(message.getClass().getCanonicalName());
            message.write(checkpointStream);
          } catch (IOException ioe) {
            LOG.error("Fail checkpointing messages to " + checkpointedPath, ioe);
            throw new RuntimeException("Failed writing to HDFS file "
                + checkpointedPath, ioe);
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("message count = " + checkpointMessageCount);
          }
        }
      }

    }

    @Override
    public void onClose() {

    }

  }

}
