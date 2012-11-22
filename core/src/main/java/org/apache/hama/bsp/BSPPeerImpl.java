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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.bsp.ft.AsyncRcvdMsgCheckpointImpl;
import org.apache.hama.bsp.ft.BSPFaultTolerantService;
import org.apache.hama.bsp.ft.FaultTolerantPeerService;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.MessageManagerFactory;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.sync.PeerSyncClient;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.KeyValuePair;

/**
 * This class represents a BSP peer.
 */
public final class BSPPeerImpl<K1, V1, K2, V2, M extends Writable> implements
    BSPPeer<K1, V1, K2, V2, M> {

  private static final Log LOG = LogFactory.getLog(BSPPeerImpl.class);

  public static enum PeerCounter {
    COMPRESSED_MESSAGES, SUPERSTEP_SUM, TASK_INPUT_RECORDS, TASK_OUTPUT_RECORDS, IO_BYTES_READ, MESSAGE_BYTES_TRANSFERED, MESSAGE_BYTES_RECEIVED, TOTAL_MESSAGES_SENT, TOTAL_MESSAGES_RECEIVED, COMPRESSED_BYTES_SENT, COMPRESSED_BYTES_RECEIVED, TIME_IN_SYNC_MS
  }

  private final Configuration conf;
  private final FileSystem fs;
  private BSPJob bspJob;

  private TaskStatus currentTaskStatus;

  private TaskAttemptID taskId;
  private BSPPeerProtocol umbilical;

  private String[] allPeers;

  // SYNC
  private PeerSyncClient syncClient;
  private MessageManager<M> messenger;

  // IO
  private int partition;
  private String splitClass;
  private BytesWritable split;
  private OutputCollector<K2, V2> collector;
  private RecordReader<K1, V1> in;
  private RecordWriter<K2, V2> outWriter;
  private final KeyValuePair<K1, V1> cachedPair = new KeyValuePair<K1, V1>();

  private InetSocketAddress peerAddress;

  private Counters counters;
  private Combiner<M> combiner;

  private FaultTolerantPeerService<M> faultToleranceService;

  private long splitSize = 0L;

  /**
   * Protected default constructor for LocalBSPRunner.
   */
  protected BSPPeerImpl() {
    conf = null;
    fs = null;
  }

  /**
   * For unit test.
   * 
   * @param conf is the configuration file.
   * @param dfs is the Hadoop FileSystem.
   */
  protected BSPPeerImpl(final Configuration conf, FileSystem dfs) {
    this.conf = conf;
    this.fs = dfs;
  }

  /**
   * For unit test.
   * 
   * @param conf is the configuration file.
   * @param dfs is the Hadoop FileSystem.
   * @param counters is the counters from outside.
   */
  public BSPPeerImpl(final Configuration conf, FileSystem dfs, Counters counters) {
    this(conf, dfs);
    this.counters = counters;
  }

  public BSPPeerImpl(BSPJob job, Configuration conf, TaskAttemptID taskId,
      BSPPeerProtocol umbilical, int partition, String splitClass,
      BytesWritable split, Counters counters) throws Exception {
    this(job, conf, taskId, umbilical, partition, splitClass, split, counters,
        -1, TaskStatus.State.RUNNING);
  }

  /**
   * BSPPeer Constructor.
   * 
   * BSPPeer acts on behalf of clients performing bsp() tasks.
   * 
   * @param conf is the configuration file containing bsp peer host, port, etc.
   * @param umbilical is the bsp protocol used to contact its parent process.
   * @param taskId is the id that current process holds.
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public BSPPeerImpl(BSPJob job, Configuration conf, TaskAttemptID taskId,
      BSPPeerProtocol umbilical, int partition, String splitClass,
      BytesWritable split, Counters counters, long superstep,
      TaskStatus.State state) throws Exception {
    this.conf = conf;
    this.taskId = taskId;
    this.umbilical = umbilical;
    this.bspJob = job;
    // IO
    this.partition = partition;
    this.splitClass = splitClass;
    this.split = split;
    this.counters = counters;

    this.fs = FileSystem.get(conf);

    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    peerAddress = new InetSocketAddress(bindAddress, bindPort);

    initializeIO();
    initializeSyncService(superstep, state);

    TaskStatus.Phase phase = TaskStatus.Phase.STARTING;
    String stateString = "running";
    if (state == TaskStatus.State.RECOVERING) {
      phase = TaskStatus.Phase.RECOVERING;
      stateString = "recovering";
    }

    setCurrentTaskStatus(new TaskStatus(taskId.getJobID(), taskId, 1.0f, state,
        stateString, peerAddress.getHostName(), phase, counters));

    initilizeMessaging();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initialized Messaging service.");
    }

    final String combinerName = conf.get("bsp.combiner.class");
    if (combinerName != null) {
      combiner = (Combiner<M>) ReflectionUtils.newInstance(
          conf.getClassByName(combinerName), conf);
    }

    if (conf.getBoolean(Constants.FAULT_TOLERANCE_FLAG, false)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fault tolerance enabled.");
      }
      if (superstep > 0)
        conf.setInt("attempt.superstep", (int) superstep);
      Class<?> ftClass = conf.getClass(Constants.FAULT_TOLERANCE_CLASS,
          AsyncRcvdMsgCheckpointImpl.class, BSPFaultTolerantService.class);
      if (ftClass != null) {
        if (superstep > 0) {
          counters.incrCounter(PeerCounter.SUPERSTEP_SUM, superstep);
        }

        this.faultToleranceService = ((BSPFaultTolerantService<M>) ReflectionUtils
            .newInstance(ftClass, null)).constructPeerFaultTolerance(job, this,
            syncClient, peerAddress, this.taskId, superstep, conf, messenger);
        TaskStatus.State newState = this.faultToleranceService
            .onPeerInitialized(state);

        if (state == TaskStatus.State.RECOVERING) {
          if (newState == TaskStatus.State.RUNNING) {
            phase = TaskStatus.Phase.STARTING;
            stateString = "running";
            state = newState;
          }

          setCurrentTaskStatus(new TaskStatus(taskId.getJobID(), taskId, 1.0f,
              state, stateString, peerAddress.getHostName(), phase, counters));
          if (LOG.isDebugEnabled()) {
            LOG.debug("State after FT service initialization - "
                + newState.toString());
          }

        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Initialized fault tolerance service");
        }
      }
    }

    doFirstSync(superstep);

    if (LOG.isDebugEnabled()) {
      LOG.info(new StringBuffer("BSP Peer successfully initialized for ")
          .append(this.taskId.toString()).append(" ").append(superstep)
          .toString());
    }
  }

  /**
   * Transfers DistributedCache files into the local cache files. Also creates
   * symbolic links for URIs specified with a fragment if
   * DistributedCache.getSymlinks() is true.
   * 
   * @throws IOException If a DistributedCache file cannot be found.
   */
  public final void moveCacheFiles() throws IOException {
    StringBuilder files = new StringBuilder();
    boolean first = true;
    if (DistributedCache.getCacheFiles(conf) != null) {
      for (URI uri : DistributedCache.getCacheFiles(conf)) {
        if (uri != null) {
          if (!first) {
            files.append(",");
          }
          if (null != uri.getFragment() && DistributedCache.getSymlink(conf)) {

            FileUtil.symLink(uri.getPath(), uri.getFragment());
            files.append(uri.getFragment()).append(",");
          }
          FileSystem hdfs = FileSystem.get(conf);
          Path pathSrc = new Path(uri.getPath());
          if (hdfs.exists(pathSrc)) {
            LocalFileSystem local = FileSystem.getLocal(conf);
            Path pathDst = new Path(local.getWorkingDirectory(),
                pathSrc.getName());
            hdfs.copyToLocalFile(pathSrc, pathDst);
            files.append(pathDst.toUri().getPath());
          }
          first = false;
        }
      }
    }
    if (files.length() > 0) {
      DistributedCache.addLocalFiles(conf, files.toString());
    }
  }

  @SuppressWarnings("unchecked")
  public final void initInput() throws IOException {
    InputSplit inputSplit = null;
    // reinstantiate the split
    try {
      if (splitClass != null) {
        inputSplit = (InputSplit) ReflectionUtils.newInstance(
            getConfiguration().getClassByName(splitClass), getConfiguration());
      }
    } catch (ClassNotFoundException exp) {
      IOException wrap = new IOException("Split class " + splitClass
          + " not found");
      wrap.initCause(exp);
      throw wrap;
    }

    if (inputSplit != null) {
      DataInputBuffer splitBuffer = new DataInputBuffer();
      splitBuffer.reset(split.getBytes(), 0, split.getLength());
      inputSplit.readFields(splitBuffer);
      if (in != null) {
        in.close();
      }
      in = new TrackedRecordReader<K1, V1>(bspJob.getInputFormat()
          .getRecordReader(inputSplit, bspJob),
          getCounter(BSPPeerImpl.PeerCounter.TASK_INPUT_RECORDS),
          getCounter(BSPPeerImpl.PeerCounter.IO_BYTES_READ));
      this.splitSize = inputSplit.getLength();
    }
  }

  /**
   * @return the size of assigned split
   */
  @Override
  public long getSplitSize() {
    return splitSize;
  }

  /**
   * @return the position in the input stream.
   */
  @Override
  public long getPos() throws IOException {
    return in.getPos();
  }

  public final void initilizeMessaging() throws ClassNotFoundException {
    messenger = MessageManagerFactory.getMessageManager(conf);
    messenger.init(taskId, this, conf, peerAddress);
  }

  public final void initializeSyncService(long superstep, TaskStatus.State state)
      throws Exception {

    syncClient = SyncServiceFactory.getPeerSyncClient(conf);
    syncClient.init(conf, taskId.getJobID(), taskId);
    syncClient.register(taskId.getJobID(), taskId, peerAddress.getHostName(),
        peerAddress.getPort());
  }

  private void doFirstSync(long superstep) throws SyncException {
    if (superstep > 0)
      --superstep;
    syncClient.enterBarrier(taskId.getJobID(), taskId, superstep);
    syncClient.leaveBarrier(taskId.getJobID(), taskId, superstep);
  }

  @SuppressWarnings("unchecked")
  public final void initializeIO() throws Exception {

    initInput();

    String outdir = null;
    if (conf.get("bsp.output.dir") != null) {
      Path outputDir = new Path(conf.get("bsp.output.dir",
          "tmp-" + System.currentTimeMillis()), Task.getOutputName(partition));
      outdir = outputDir.makeQualified(fs).toString();
    }
    outWriter = bspJob.getOutputFormat().getRecordWriter(fs, bspJob, outdir);
    final RecordWriter<K2, V2> finalOut = outWriter;

    collector = new OutputCollector<K2, V2>() {
      @Override
      public void collect(K2 key, V2 value) throws IOException {
        finalOut.write(key, value);
      }
    };

    try {
      moveCacheFiles();
    } catch (Exception e) {
      LOG.error(e);
    }

  }

  @Override
  public final M getCurrentMessage() throws IOException {
    return messenger.getCurrentMessage();
  }

  @Override
  public final void send(String peerName, M msg) throws IOException {
    messenger.send(peerName, msg);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#sync()
   */
  @Override
  public final void sync() throws IOException, SyncException,
      InterruptedException {

    // normally all messages should been send now, finalizing the send phase
    messenger.finishSendPhase();
    Iterator<Entry<InetSocketAddress, MessageQueue<M>>> it = messenger
        .getMessageIterator();

    while (it.hasNext()) {
      Entry<InetSocketAddress, MessageQueue<M>> entry = it.next();
      final InetSocketAddress addr = entry.getKey();
      final Iterable<M> messages = entry.getValue();

      final BSPMessageBundle<M> bundle = combineMessages(messages);
      // remove this message during runtime to save a bit of memory
      it.remove();
      try {
        messenger.transfer(addr, bundle);
      } catch (Exception e) {
        LOG.error("Error while sending messages", e);
      }
    }

    if (this.faultToleranceService != null) {
      try {
        this.faultToleranceService.beforeBarrier();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    long startBarrier = System.currentTimeMillis();
    enterBarrier();

    if (this.faultToleranceService != null) {
      try {
        this.faultToleranceService.duringBarrier();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    // Clear outgoing queues.
    messenger.clearOutgoingQueues();

    leaveBarrier();

    incrementCounter(PeerCounter.TIME_IN_SYNC_MS,
        (System.currentTimeMillis() - startBarrier));
    incrementCounter(PeerCounter.SUPERSTEP_SUM, 1L);

    currentTaskStatus.setCounters(counters);

    if (this.faultToleranceService != null) {
      try {
        this.faultToleranceService.afterBarrier();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    umbilical.statusUpdate(taskId, currentTaskStatus);

  }

  private final BSPMessageBundle<M> combineMessages(Iterable<M> messages) {
    BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
    if (combiner != null) {
      bundle.addMessage(combiner.combine(messages));
    } else {
      for (M message : messages) {
        bundle.addMessage(message);
      }
    }
    return bundle;
  }

  protected final void enterBarrier() throws SyncException {
    syncClient.enterBarrier(taskId.getJobID(), taskId,
        currentTaskStatus.getSuperstepCount());
  }

  protected final void leaveBarrier() throws SyncException {
    syncClient.leaveBarrier(taskId.getJobID(), taskId,
        currentTaskStatus.getSuperstepCount());
  }

  /**
   * Delete files from the local cache
   * 
   * @throws IOException If a DistributedCache file cannot be found.
   */
  public void deleteLocalFiles() throws IOException {
    if (DistributedCache.getLocalCacheFiles(conf) != null) {
      for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
        if (path != null) {
          LocalFileSystem local = FileSystem.getLocal(conf);
          if (local.exists(path)) {
            local.delete(path, true); // recursive true
          }
        }
      }
    }
    DistributedCache.setLocalFiles(conf, "");
  }

  public final void close() {
    // there are many catches, because we want to close always every component
    // even if the one before failed.
    if (in != null) {
      try {
        in.close();
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    if (outWriter != null) {
      try {
        outWriter.close();
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    this.clear();
    try {
      syncClient.close();
    } catch (Exception e) {
      LOG.error(e);
    }
    try {
      messenger.close();
    } catch (Exception e) {
      LOG.error(e);
    }
    // Delete files from the local cache
    try {
      deleteLocalFiles();
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  @Override
  public final void clear() {
    messenger.clearOutgoingQueues();
  }

  /**
   * @return the string as host:port of this Peer
   */
  @Override
  public final String getPeerName() {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  @Override
  public final String[] getAllPeerNames() {
    initPeerNames();
    return allPeers;
  }

  @Override
  public final String getPeerName(int index) {
    initPeerNames();
    return allPeers[index];
  }

  @Override
  public int getPeerIndex() {
    return this.taskId.getTaskID().getId();
  }

  @Override
  public final int getNumPeers() {
    initPeerNames();
    return allPeers.length;
  }

  private final void initPeerNames() {
    if (allPeers == null) {
      allPeers = syncClient.getAllPeerNames(taskId);
    }
  }

  /**
   * @return the number of messages
   */
  @Override
  public final int getNumCurrentMessages() {
    return messenger.getNumCurrentMessages();
  }

  /**
   * Sets the current status
   * 
   * @param currentTaskStatus the new task status to set
   */
  public final void setCurrentTaskStatus(TaskStatus currentTaskStatus) {
    this.currentTaskStatus = currentTaskStatus;
  }

  /**
   * @return the count of current super-step
   */
  @Override
  public final long getSuperstepCount() {
    return currentTaskStatus.getSuperstepCount();
  }

  /**
   * Gets the job configuration.
   * 
   * @return the conf
   */
  @Override
  public final Configuration getConfiguration() {
    return conf;
  }

  /*
   * IO STUFF
   */

  @Override
  public final void write(K2 key, V2 value) throws IOException {
    incrementCounter(PeerCounter.TASK_OUTPUT_RECORDS, 1);
    collector.collect(key, value);
  }

  @Override
  public final boolean readNext(K1 key, V1 value) throws IOException {
    return in.next(key, value);
  }

  @Override
  public final KeyValuePair<K1, V1> readNext() throws IOException {
    K1 k = in.createKey();
    V1 v = in.createValue();
    if (in.next(k, v)) {
      cachedPair.clear();
      cachedPair.setKey(k);
      cachedPair.setValue(v);
      return cachedPair;
    } else {
      return null;
    }
  }

  @Override
  public final void reopenInput() throws IOException {
    initInput();
  }

  @Override
  public final Counter getCounter(Enum<?> name) {
    return counters == null ? null : counters.findCounter(name);
  }

  @Override
  public final Counter getCounter(String group, String name) {
    Counters.Counter counter = null;
    if (counters != null) {
      counter = counters.findCounter(group, name);
    }
    return counter;
  }

  public Counters getCounters() {
    return counters;
  }

  @Override
  public final void incrementCounter(Enum<?> key, long amount) {
    if (counters != null) {
      counters.incrCounter(key, amount);
    }
  }

  @Override
  public final void incrementCounter(String group, String counter, long amount) {
    if (counters != null) {
      counters.incrCounter(group, counter, amount);
    }
  }

  @Override
  public TaskAttemptID getTaskId() {
    return taskId;
  }

}
