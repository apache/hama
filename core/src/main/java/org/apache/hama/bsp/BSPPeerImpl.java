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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.MessageManagerFactory;
import org.apache.hama.bsp.message.MessageQueue;
import org.apache.hama.bsp.sync.SyncClient;
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
    SUPERSTEP_SUM, SUPERSTEPS, TASK_INPUT_RECORDS, TASK_OUTPUT_RECORDS,
    IO_BYTES_READ, MESSAGE_BYTES_TRANSFERED, MESSAGE_BYTES_RECEIVED,
    TOTAL_MESSAGES_SENT, TOTAL_MESSAGES_RECEIVED, COMPRESSED_BYTES_SENT,
    COMPRESSED_BYTES_RECEIVED, TIME_IN_SYNC_MS
  }

  private final Configuration conf;
  private final FileSystem fs;
  private BSPJob bspJob;

  private TaskStatus currentTaskStatus;

  private TaskAttemptID taskId;
  private BSPPeerProtocol umbilical;

  private String[] allPeers;

  // SYNC
  private SyncClient syncClient;
  private MessageManager<M> messenger;

  // A checkpoint is initiated at the <checkPointInterval>th interval.
  private int checkPointInterval;
  private long lastCheckPointStep;

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
      BytesWritable split, Counters counters) throws Exception {
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

    this.checkPointInterval = conf.getInt(Constants.CHECKPOINT_INTERVAL,
        Constants.DEFAULT_CHECKPOINT_INTERVAL);
    this.lastCheckPointStep = 0;

    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    peerAddress = new InetSocketAddress(bindAddress, bindPort);
    initialize();
    syncClient.register(taskId.getJobID(), taskId, peerAddress.getHostName(),
        peerAddress.getPort());
    // initial barrier syncing to get all the hosts to the same point, to get
    // consistent peernames.
    syncClient.enterBarrier(taskId.getJobID(), taskId, -1);
    syncClient.leaveBarrier(taskId.getJobID(), taskId, -1);
    setCurrentTaskStatus(new TaskStatus(taskId.getJobID(), taskId, 1.0f,
        TaskStatus.State.RUNNING, "running", peerAddress.getHostName(),
        TaskStatus.Phase.STARTING, counters));

    messenger = MessageManagerFactory.getMessageManager(conf);
    messenger.init(taskId, this, conf, peerAddress);

    final String combinerName = conf.get("bsp.combiner.class");
    if (combinerName != null) {
      combiner = (Combiner<M>) ReflectionUtils.newInstance(
          conf.getClassByName(combinerName), conf);
    }

  }

  @SuppressWarnings("unchecked")
  public final void initialize() throws Exception {
    syncClient = SyncServiceFactory.getSyncClient(conf);
    syncClient.init(conf, taskId.getJobID(), taskId);

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
      public void collect(K2 key, V2 value) throws IOException {
        finalOut.write(key, value);
      }
    };

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
    }
  }

  @Override
  public final M getCurrentMessage() throws IOException {
    return messenger.getCurrentMessage();
  }

  @Override
  public final void send(String peerName, M msg) throws IOException {
    incrementCounter(PeerCounter.TOTAL_MESSAGES_SENT, 1L);
    messenger.send(peerName, msg);
  }

  /*
   * returns true if the peer would checkpoint in the next sync.
   */
  public final boolean isReadyToCheckpoint() {

    checkPointInterval = conf.getInt(Constants.CHECKPOINT_INTERVAL, 1);
    if (LOG.isDebugEnabled())
      LOG.debug(new StringBuffer(1000).append("Enabled = ")
          .append(conf.getBoolean(Constants.CHECKPOINT_ENABLED, false))
          .append(" checkPointInterval = ").append(checkPointInterval)
          .append(" lastCheckPointStep = ").append(lastCheckPointStep)
          .append(" getSuperstepCount() = ").append(getSuperstepCount())
          .toString());

    return (conf.getBoolean(Constants.CHECKPOINT_ENABLED, false)
        && (checkPointInterval != 0) && (((int) (getSuperstepCount() - lastCheckPointStep)) >= checkPointInterval));

  }

  private final String checkpointedPath() {
    String backup = conf.get("bsp.checkpoint.prefix_path", "/checkpoint/");
    String ckptPath = backup + bspJob.getJobID().toString() + "/"
        + getSuperstepCount() + "/" + this.taskId.toString();
    if (LOG.isDebugEnabled())
      LOG.debug("Messages are to be saved to " + ckptPath);
    return ckptPath;
  }

  final void checkpoint(String checkpointedPath, BSPMessageBundle<M> bundle) {
    FSDataOutputStream out = null;
    try {
      out = this.fs.create(new Path(checkpointedPath));
      bundle.write(out);
    } catch (IOException ioe) {
      LOG.warn("Fail checkpointing messages to " + checkpointedPath, ioe);
    } finally {
      try {
        if (null != out)
          out.close();
      } catch (IOException e) {
        LOG.warn("Fail to close dfs output stream while checkpointing.", e);
      }
    }
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.bsp.BSPPeerInterface#sync()
   */
  @Override
  public final void sync() throws IOException, SyncException,
      InterruptedException {
    long startBarrier = System.currentTimeMillis();
    enterBarrier();
    // normally all messages should been send now, finalizing the send phase
    messenger.finishSendPhase();
    Iterator<Entry<InetSocketAddress, MessageQueue<M>>> it = messenger
        .getMessageIterator();

    boolean shouldCheckPoint = false;

    if ((shouldCheckPoint = isReadyToCheckpoint())) {
      lastCheckPointStep = getSuperstepCount();
    }

    while (it.hasNext()) {
      Entry<InetSocketAddress, MessageQueue<M>> entry = it.next();
      final InetSocketAddress addr = entry.getKey();
      final Iterable<M> messages = entry.getValue();

      final BSPMessageBundle<M> bundle = combineMessages(messages);

      if (shouldCheckPoint) {
        checkpoint(checkpointedPath(), bundle);
      }

      // remove this message during runtime to save a bit of memory
      it.remove();

      messenger.transfer(addr, bundle);
    }

    leaveBarrier();

    incrementCounter(PeerCounter.TIME_IN_SYNC_MS,
        (System.currentTimeMillis() - startBarrier));
    incrementCounter(PeerCounter.SUPERSTEP_SUM, 1L);

    currentTaskStatus.setCounters(counters);

    umbilical.statusUpdate(taskId, currentTaskStatus);
    // Clear outgoing queues.
    messenger.clearOutgoingQueues();
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

  public final void close() throws SyncException, IOException,
      InterruptedException {
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
  }

  @Override
  public final void clear() {
    messenger.clearOutgoingQueues();
  }

  /**
   * @return the string as host:port of this Peer
   */
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
   * @param currentTaskStatus
   */
  public final void setCurrentTaskStatus(TaskStatus currentTaskStatus) {
    this.currentTaskStatus = currentTaskStatus;
  }

  /**
   * @return the count of current super-step
   */
  public final long getSuperstepCount() {
    return currentTaskStatus.getSuperstepCount();
  }

  /**
   * Gets the job configuration.
   * 
   * @return the conf
   */
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

}
