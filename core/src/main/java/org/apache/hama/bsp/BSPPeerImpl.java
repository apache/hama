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
import java.util.LinkedList;
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

  protected static enum PeerCounter {
    SUPERSTEPS
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

  // IO
  private int partition;
  private String splitClass;
  private BytesWritable split;
  private OutputCollector<K2, V2> collector;
  private RecordReader<K1, V1> in;
  private RecordWriter<K2, V2> outWriter;

  private InetSocketAddress peerAddress;
  private Counters counters;

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
   * BSPPeer Constructor.
   * 
   * BSPPeer acts on behalf of clients performing bsp() tasks.
   * 
   * @param conf is the configuration file containing bsp peer host, port, etc.
   * @param umbilical is the bsp protocol used to contact its parent process.
   * @param taskId is the id that current process holds.
   * @throws Exception
   */
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
    setCurrentTaskStatus(new TaskStatus(taskId.getJobID(), taskId, 0,
        TaskStatus.State.RUNNING, "running", peerAddress.getHostName(),
        TaskStatus.Phase.STARTING, counters));

    messenger = MessageManagerFactory.getMessageManager(conf);
    messenger.init(conf, peerAddress);

  }

  @SuppressWarnings("unchecked")
  public final void initialize() throws Exception {
    syncClient = SyncServiceFactory.getSyncClient(conf);
    syncClient.init(conf, taskId.getJobID(), taskId);

    initInput();

    // just output something when the user configured it
    if (conf.get("bsp.output.dir") != null) {
      Path outdir = new Path(conf.get("bsp.output.dir"),
          Task.getOutputName(partition));
      outWriter = bspJob.getOutputFormat().getRecordWriter(fs, bspJob,
          outdir.makeQualified(fs).toString());
      final RecordWriter<K2, V2> finalOut = outWriter;

      collector = new OutputCollector<K2, V2>() {
        public void collect(K2 key, V2 value) throws IOException {
          finalOut.write(key, value);
        }
      };
    }

  }

  @SuppressWarnings("unchecked")
  public final void initInput() throws IOException {
    // just read input if the user defined one
    if (conf.get("bsp.input.dir") != null) {
      InputSplit inputSplit = null;
      // reinstantiate the split
      try {
        inputSplit = (InputSplit) ReflectionUtils.newInstance(
            getConfiguration().getClassByName(splitClass), getConfiguration());
      } catch (ClassNotFoundException exp) {
        IOException wrap = new IOException("Split class " + splitClass
            + " not found");
        wrap.initCause(exp);
        throw wrap;
      }

      DataInputBuffer splitBuffer = new DataInputBuffer();
      splitBuffer.reset(split.getBytes(), 0, split.getLength());
      inputSplit.readFields(splitBuffer);
      if (in != null) {
        in.close();
      }
      in = bspJob.getInputFormat().getRecordReader(inputSplit, bspJob);
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
    enterBarrier();
    Iterator<Entry<InetSocketAddress, LinkedList<M>>> it = messenger
        .getMessageIterator();

    while (it.hasNext()) {
      Entry<InetSocketAddress, LinkedList<M>> entry = it.next();
      final InetSocketAddress addr = entry.getKey();
      final Iterable<M> messages = entry.getValue();

      final BSPMessageBundle<M> bundle = combineMessages(messages);

      if (conf.getBoolean("bsp.checkpoint.enabled", false)) {
        checkpoint(checkpointedPath(), bundle);
      }

      // remove this message during runtime to save a bit of memory
      it.remove();

      messenger.transfer(addr, bundle);
    }

    leaveBarrier();

    incrCounter(PeerCounter.SUPERSTEPS, 1);
    currentTaskStatus.setCounters(counters);

    umbilical.statusUpdate(taskId, currentTaskStatus);
    // Clear outgoing queues.
    messenger.clearOutgoingQueues();
  }

  private final BSPMessageBundle<M> combineMessages(Iterable<M> messages) {
    if (!conf.getClass("bsp.combiner.class", Combiner.class).equals(
        Combiner.class)) {
      Combiner<M> combiner = (Combiner<M>) ReflectionUtils.newInstance(
          conf.getClass("bsp.combiner.class", Combiner.class), conf);

      return combiner.combine(messages);
    } else {
      BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
      for (M message : messages) {
        bundle.addMessage(message);
      }
      return bundle;
    }
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
    if (in != null) {
      in.close();
    }
    if (outWriter != null) {
      outWriter.close();
    }
    this.clear();
    syncClient.close();

    messenger.close();
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
      return new KeyValuePair<K1, V1>(k, v);
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

  @Override
  public final void incrCounter(Enum<?> key, long amount) {
    if (counters != null) {
      counters.incrCounter(key, amount);
    }
  }

  @Override
  public final void incrCounter(String group, String counter, long amount) {
    if (counters != null) {
      counters.incrCounter(group, counter, amount);
    }
  }
}
