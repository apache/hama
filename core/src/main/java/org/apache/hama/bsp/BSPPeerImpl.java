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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.MessageManagerFactory;
import org.apache.hama.bsp.sync.SyncClient;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.KeyValuePair;

/**
 * This class represents a BSP peer.
 */
public class BSPPeerImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
    BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  private static final Log LOG = LogFactory.getLog(BSPPeerImpl.class);

  private final Configuration conf;
  private final FileSystem fs;
  private BSPJob bspJob;

  private TaskStatus currentTaskStatus;

  private TaskAttemptID taskId;
  private BSPPeerProtocol umbilical;

  private String[] allPeers;

  // SYNC
  private SyncClient syncClient;
  private MessageManager messenger;

  // IO
  private int partition;
  private String splitClass;
  private BytesWritable split;
  private OutputCollector<KEYOUT, VALUEOUT> collector;
  private RecordReader<KEYIN, VALUEIN> in;
  private RecordWriter<KEYOUT, VALUEOUT> outWriter;

  private InetSocketAddress peerAddress;

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
      BytesWritable split) throws Exception {
    this.conf = conf;
    this.taskId = taskId;
    this.umbilical = umbilical;
    this.bspJob = job;
    // IO
    this.partition = partition;
    this.splitClass = splitClass;
    this.split = split;

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
        TaskStatus.Phase.STARTING));

    messenger = MessageManagerFactory.getMessageManager(conf);
    messenger.init(conf, peerAddress);

  }

  @SuppressWarnings("unchecked")
  public void initialize() throws Exception {
    syncClient = SyncServiceFactory.getSyncClient(conf);
    syncClient.init(conf, taskId.getJobID(), taskId);

    initInput();

    // just output something when the user configured it
    if (conf.get("bsp.output.dir") != null) {
      Path outdir = new Path(conf.get("bsp.output.dir"),
          Task.getOutputName(partition));
      outWriter = bspJob.getOutputFormat().getRecordWriter(fs, bspJob,
          outdir.makeQualified(fs).toString());
      final RecordWriter<KEYOUT, VALUEOUT> finalOut = outWriter;

      collector = new OutputCollector<KEYOUT, VALUEOUT>() {
        public void collect(KEYOUT key, VALUEOUT value) throws IOException {
          finalOut.write(key, value);
        }
      };
    }

  }

  @SuppressWarnings("unchecked")
  public void initInput() throws IOException {
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
  public BSPMessage getCurrentMessage() throws IOException {
    return messenger.getCurrentMessage();
  }

  @Override
  public void send(String peerName, BSPMessage msg) throws IOException {
    messenger.send(peerName, msg);
  }

  private String checkpointedPath() {
    String backup = conf.get("bsp.checkpoint.prefix_path", "/checkpoint/");
    String ckptPath = backup + bspJob.getJobID().toString() + "/"
        + getSuperstepCount() + "/" + this.taskId.toString();
    if (LOG.isDebugEnabled())
      LOG.debug("Messages are to be saved to " + ckptPath);
    return ckptPath;
  }

  void checkpoint(String checkpointedPath, BSPMessageBundle bundle) {
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
  public void sync() throws InterruptedException {
    try {
      enterBarrier();
      Iterator<Entry<InetSocketAddress, LinkedList<BSPMessage>>> it = messenger
          .getMessageIterator();

      while (it.hasNext()) {
        Entry<InetSocketAddress, LinkedList<BSPMessage>> entry = it.next();
        final InetSocketAddress addr = entry.getKey();
        final Iterable<BSPMessage> messages = entry.getValue();

        final BSPMessageBundle bundle = combineMessages(messages);

        if (conf.getBoolean("bsp.checkpoint.enabled", false)) {
          checkpoint(checkpointedPath(), bundle);
        }

        // remove this message during runtime to save a bit of memory
        it.remove();

        messenger.transfer(addr, bundle);
      }

      leaveBarrier();
      currentTaskStatus.incrementSuperstepCount();
      umbilical.statusUpdate(taskId, currentTaskStatus);

      // Clear outgoing queues.
      messenger.clearOutgoingQueues();

    } catch (Exception e) {
      LOG.fatal(
          "Caught exception during superstep "
              + currentTaskStatus.getSuperstepCount() + "!", e);
    }
  }

  private BSPMessageBundle combineMessages(Iterable<BSPMessage> messages) {
    if (!conf.getClass("bsp.combiner.class", Combiner.class).equals(
        Combiner.class)) {
      Combiner combiner = (Combiner) ReflectionUtils.newInstance(
          conf.getClass("bsp.combiner.class", Combiner.class), conf);

      return combiner.combine(messages);
    } else {
      BSPMessageBundle bundle = new BSPMessageBundle();
      for (BSPMessage message : messages) {
        bundle.addMessage(message);
      }
      return bundle;
    }
  }

  protected void enterBarrier() throws Exception {
    syncClient.enterBarrier(taskId.getJobID(), taskId,
        currentTaskStatus.getSuperstepCount());
  }

  protected void leaveBarrier() throws Exception {
    syncClient.leaveBarrier(taskId.getJobID(), taskId,
        currentTaskStatus.getSuperstepCount());
  }

  public void close() throws Exception {
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
  public void clear() {
    messenger.clearOutgoingQueues();
  }

  /**
   * @return the string as host:port of this Peer
   */
  public String getPeerName() {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  @Override
  public String[] getAllPeerNames() {
    initPeerNames();
    return allPeers;
  }

  @Override
  public String getPeerName(int index) {
    initPeerNames();
    return allPeers[index];
  }

  @Override
  public int getNumPeers() {
    initPeerNames();
    return allPeers.length;
  }

  private void initPeerNames() {
    if (allPeers == null) {
      allPeers = syncClient.getAllPeerNames(taskId);
    }
  }

  /**
   * @return the number of messages
   */
  @Override
  public int getNumCurrentMessages() {
    return messenger.getNumCurrentMessages();
  }

  /**
   * Sets the current status
   * 
   * @param currentTaskStatus
   */
  public void setCurrentTaskStatus(TaskStatus currentTaskStatus) {
    this.currentTaskStatus = currentTaskStatus;
  }

  /**
   * @return the count of current super-step
   */
  public long getSuperstepCount() {
    return currentTaskStatus.getSuperstepCount();
  }

  /**
   * Gets the job configuration.
   * 
   * @return the conf
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /*
   * IO STUFF
   */

  @Override
  public void write(KEYOUT key, VALUEOUT value) throws IOException {
    collector.collect(key, value);
  }

  @Override
  public boolean readNext(KEYIN key, VALUEIN value) throws IOException {
    return in.next(key, value);
  }

  @Override
  public KeyValuePair<KEYIN, VALUEIN> readNext() throws IOException {
    KEYIN k = in.createKey();
    VALUEIN v = in.createValue();
    if (in.next(k, v)) {
      return new KeyValuePair<KEYIN, VALUEIN>(k, v);
    } else {
      return null;
    }
  }

  @Override
  public void reopenInput() throws IOException {
    initInput();
  }

}
