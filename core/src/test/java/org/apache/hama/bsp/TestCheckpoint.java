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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.bsp.ft.AsyncRcvdMsgCheckpointImpl;
import org.apache.hama.bsp.ft.FaultTolerantPeerService;
import org.apache.hama.bsp.message.MessageEventListener;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.bsp.sync.BSPPeerSyncClient;
import org.apache.hama.bsp.sync.PeerSyncClient;
import org.apache.hama.bsp.sync.SyncEvent;
import org.apache.hama.bsp.sync.SyncEventListener;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.bsp.sync.SyncServiceFactory;
import org.apache.hama.util.BSPNetUtils;
import org.apache.hama.util.KeyValuePair;

public class TestCheckpoint extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestCheckpoint.class);

  static final String checkpointedDir = "checkpoint/job_201110302255_0001/0/";

  public static class TestMessageManager implements MessageManager<Text> {

    List<Text> messageQueue = new ArrayList<Text>();
    BSPMessageBundle<Text> loopbackBundle = new BSPMessageBundle<Text>();
    Iterator<Text> iter = null;
    MessageEventListener<Text> listener;

    @Override
    public void init(TaskAttemptID attemptId, BSPPeer<?, ?, ?, ?, Text> peer,
        Configuration conf, InetSocketAddress peerAddress) {
      // TODO Auto-generated method stub

    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    @Override
    public Text getCurrentMessage() throws IOException {
      if (iter == null)
        iter = this.messageQueue.iterator();
      if (iter.hasNext())
        return iter.next();
      return null;
    }

    @Override
    public void send(String peerName, Text msg) throws IOException {
    }

    @Override
    public void finishSendPhase() throws IOException {
    }

    @Override
    public Iterator<Entry<InetSocketAddress, MessageQueue<Text>>> getMessageIterator() {
      return null;
    }

    @Override
    public void transfer(InetSocketAddress addr, BSPMessageBundle<Text> bundle)
        throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void clearOutgoingQueues() {
    }

    @Override
    public int getNumCurrentMessages() {
      return this.messageQueue.size();
    }

    public BSPMessageBundle<Text> getLoopbackBundle() {
      return this.loopbackBundle;
    }

    public void addMessage(Text message) throws IOException {
      this.messageQueue.add(message);
      listener.onMessageReceived(message);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void loopBackMessages(BSPMessageBundle<? extends Writable> bundle) {
      this.loopbackBundle = (BSPMessageBundle<Text>) bundle;
    }

    @Override
    public void loopBackMessage(Writable message) {
    }

    @Override
    public void registerListener(MessageEventListener<Text> listener)
        throws IOException {
      this.listener = listener;
    }

  }

  public static class TestBSPPeer implements
      BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable, Text> {

    Configuration conf;
    long superstepCount;
    FaultTolerantPeerService<Text> fService;

    public TestBSPPeer(BSPJob job, Configuration conf, TaskAttemptID taskId,
        Counters counters, long superstep, BSPPeerSyncClient syncClient,
        MessageManager<Text> messenger, TaskStatus.State state) {
      this.conf = conf;
      if (superstep > 0)
        superstepCount = superstep;
      else
        superstepCount = 0L;

      try {
        fService = (new AsyncRcvdMsgCheckpointImpl<Text>())
            .constructPeerFaultTolerance(job, this, syncClient, null, taskId,
                superstep, conf, messenger);
        this.fService.onPeerInitialized(state);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void send(String peerName, Text msg) throws IOException {
    }

    @Override
    public Text getCurrentMessage() throws IOException {
      return new Text("data");
    }

    @Override
    public int getNumCurrentMessages() {
      return 1;
    }

    @Override
    public void sync() throws IOException, SyncException, InterruptedException {
      ++superstepCount;
      try {
        this.fService.afterBarrier();
      } catch (Exception e) {
        e.printStackTrace();
      }
      LOG.info("After barrier " + superstepCount);
    }

    @Override
    public long getSuperstepCount() {
      return superstepCount;
    }

    @Override
    public String getPeerName() {
      return null;
    }

    @Override
    public String getPeerName(int index) {
      return null;
    }

    @Override
    public int getPeerIndex() {
      return 1;
    }

    @Override
    public String[] getAllPeerNames() {
      return null;
    }

    @Override
    public int getNumPeers() {
      return 0;
    }

    @Override
    public void clear() {

    }

    @Override
    public void write(NullWritable key, NullWritable value) throws IOException {

    }

    @Override
    public boolean readNext(NullWritable key, NullWritable value)
        throws IOException {
      return false;
    }

    @Override
    public KeyValuePair<NullWritable, NullWritable> readNext()
        throws IOException {
      return null;
    }

    @Override
    public void reopenInput() throws IOException {

    }

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public Counter getCounter(Enum<?> name) {
      return null;
    }

    @Override
    public Counter getCounter(String group, String name) {
      return null;
    }

    @Override
    public void incrementCounter(Enum<?> key, long amount) {

    }

    @Override
    public void incrementCounter(String group, String counter, long amount) {

    }

    @Override
    public long getSplitSize() {
      return 0;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public TaskAttemptID getTaskId() {
      return null;
    }

  }

  public static class TempSyncClient extends BSPPeerSyncClient {

    Map<String, Writable> valueMap = new HashMap<String, Writable>();

    @Override
    public String constructKey(BSPJobID jobId, String... args) {
      StringBuffer buffer = new StringBuffer(100);
      buffer.append(jobId.toString()).append("/");
      for (String arg : args) {
        buffer.append(arg).append("/");
      }
      return buffer.toString();
    }

    @Override
    public boolean storeInformation(String key, Writable value,
        boolean permanent, SyncEventListener listener) {
      ArrayWritable writables = (ArrayWritable) value;
      long step = ((LongWritable) writables.get()[0]).get();
      long count = ((LongWritable) writables.get()[1]).get();

      LOG.info("SyncClient Storing value step = " + step + " count = " + count
          + " for key " + key);
      valueMap.put(key, value);
      return true;
    }

    @Override
    public boolean getInformation(String key, Writable valueHolder) {
      LOG.info("Getting value for key " + key);
      if (!valueMap.containsKey(key)) {
        return false;
      }
      Writable value = valueMap.get(key);
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream outputStream = new DataOutputStream(byteStream);
      byte[] data = null;
      try {
        value.write(outputStream);
        outputStream.flush();
        data = byteStream.toByteArray();
        ByteArrayInputStream istream = new ByteArrayInputStream(data);
        DataInputStream diStream = new DataInputStream(istream);
        valueHolder.readFields(diStream);
        return true;
      } catch (IOException e) {
        LOG.error("Error writing data to write buffer.", e);
      } finally {
        try {
          byteStream.close();
          outputStream.close();
        } catch (IOException e) {
          LOG.error("Error closing byte stream.", e);
        }
      }
      return false;
    }

    @Override
    public boolean addKey(String key, boolean permanent,
        SyncEventListener listener) {
      valueMap.put(key, NullWritable.get());
      return true;
    }

    @Override
    public boolean hasKey(String key) {
      return valueMap.containsKey(key);
    }

    @Override
    public String[] getChildKeySet(String key, SyncEventListener listener) {
      List<String> list = new ArrayList<String>();
      Iterator<String> keyIter = valueMap.keySet().iterator();
      while (keyIter.hasNext()) {
        String keyVal = keyIter.next();
        if (keyVal.startsWith(key + "/")) {
          list.add(keyVal);
        }
      }
      String[] arr = new String[list.size()];
      list.toArray(arr);
      return arr;
    }

    @Override
    public boolean registerListener(String key, SyncEvent event,
        SyncEventListener listener) {
      return false;
    }

    @Override
    public boolean remove(String key, SyncEventListener listener) {
      valueMap.remove(key);
      return false;
    }

    @Override
    public void init(Configuration conf, BSPJobID jobId, TaskAttemptID taskId)
        throws Exception {
    }

    @Override
    public void enterBarrier(BSPJobID jobId, TaskAttemptID taskId,
        long superstep) throws SyncException {
      LOG.info("Enter barrier called - " + superstep);
    }

    @Override
    public void leaveBarrier(BSPJobID jobId, TaskAttemptID taskId,
        long superstep) throws SyncException {
      LOG.info("Exit barrier called - " + superstep);
    }

    @Override
    public void register(BSPJobID jobId, TaskAttemptID taskId,
        String hostAddress, long port) {
    }

    @Override
    public String[] getAllPeerNames(TaskAttemptID taskId) {
      return null;
    }

    @Override
    public void deregisterFromBarrier(BSPJobID jobId, TaskAttemptID taskId,
        String hostAddress, long port) {
    }

    @Override
    public void stopServer() {
    }

    @Override
    public void close() throws IOException {
    }

  }

  private static void checkSuperstepMsgCount(PeerSyncClient syncClient,
      @SuppressWarnings("rawtypes")
      BSPPeer bspTask, BSPJob job, long step, long count) {

    ArrayWritable writableVal = new ArrayWritable(LongWritable.class);

    boolean result = syncClient.getInformation(
        syncClient.constructKey(job.getJobID(), "checkpoint",
            "" + bspTask.getPeerIndex()), writableVal);

    assertTrue(result);

    LongWritable superstepNo = (LongWritable) writableVal.get()[0];
    LongWritable msgCount = (LongWritable) writableVal.get()[1];

    assertEquals(step, superstepNo.get());
    assertEquals(count, msgCount.get());
  }

  public void testCheckpointInterval() throws Exception {
    Configuration config = new Configuration();
    System.setProperty("user.dir", "/tmp");
    config.set(SyncServiceFactory.SYNC_PEER_CLASS,
        TempSyncClient.class.getName());
    config.set(Constants.FAULT_TOLERANCE_CLASS,
        AsyncRcvdMsgCheckpointImpl.class.getName());
    config.setBoolean(Constants.FAULT_TOLERANCE_FLAG, true);
    config.setBoolean(Constants.CHECKPOINT_ENABLED, true);
    config.setInt(Constants.CHECKPOINT_INTERVAL, 2);
    config.set("bsp.output.dir", "/tmp/hama-test_out");
    config.set("bsp.local.dir", "/tmp/hama-test");

    FileSystem dfs = FileSystem.get(config);
    BSPJob job = new BSPJob(new BSPJobID("checkpttest", 1), "/tmp");
    TaskAttemptID taskId = new TaskAttemptID(new TaskID(job.getJobID(), 1), 1);

    TestMessageManager messenger = new TestMessageManager();
    PeerSyncClient syncClient = SyncServiceFactory.getPeerSyncClient(config);
    @SuppressWarnings("rawtypes")
    BSPPeer bspTask = new TestBSPPeer(job, config, taskId, new Counters(), -1L,
        (BSPPeerSyncClient) syncClient, messenger, TaskStatus.State.RUNNING);

    assertNotNull("BSPPeerImpl should not be null.", bspTask);

    LOG.info("Created bsp peer and other parameters");
    int port = BSPNetUtils.getFreePort(12502);
    LOG.info("Got port = " + port);

    boolean result = syncClient
        .getInformation(syncClient.constructKey(job.getJobID(), "checkpoint",
            "" + bspTask.getPeerIndex()), new ArrayWritable(LongWritable.class));

    assertFalse(result);

    bspTask.sync();
    // Superstep 1

    checkSuperstepMsgCount(syncClient, bspTask, job, 1L, 0L);

    Text txtMessage = new Text("data");
    messenger.addMessage(txtMessage);

    bspTask.sync();
    // Superstep 2

    checkSuperstepMsgCount(syncClient, bspTask, job, 1L, 0L);

    messenger.addMessage(txtMessage);

    bspTask.sync();
    // Superstep 3

    checkSuperstepMsgCount(syncClient, bspTask, job, 3L, 1L);

    bspTask.sync();
    // Superstep 4

    checkSuperstepMsgCount(syncClient, bspTask, job, 3L, 1L);

    messenger.addMessage(txtMessage);
    messenger.addMessage(txtMessage);

    bspTask.sync();
    // Superstep 5

    checkSuperstepMsgCount(syncClient, bspTask, job, 5L, 2L);

    bspTask.sync();
    // Superstep 6

    checkSuperstepMsgCount(syncClient, bspTask, job, 5L, 2L);

    dfs.delete(new Path("checkpoint"), true);
  }

  @SuppressWarnings("rawtypes")
  public void testCheckpoint() throws Exception {
    Configuration config = new Configuration();
    config.set(SyncServiceFactory.SYNC_PEER_CLASS,
        TempSyncClient.class.getName());
    config.setBoolean(Constants.FAULT_TOLERANCE_FLAG, true);
    config.set(Constants.FAULT_TOLERANCE_CLASS,
        AsyncRcvdMsgCheckpointImpl.class.getName());
    config.setBoolean(Constants.CHECKPOINT_ENABLED, true);
    int port = BSPNetUtils.getFreePort(12502);
    LOG.info("Got port = " + port);

    config.set(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST);
    config.setInt(Constants.PEER_PORT, port);

    config.set("bsp.output.dir", "/tmp/hama-test_out");
    config.set("bsp.local.dir", "/tmp/hama-test");

    FileSystem dfs = FileSystem.get(config);
    BSPJob job = new BSPJob(new BSPJobID("checkpttest", 1), "/tmp");
    TaskAttemptID taskId = new TaskAttemptID(new TaskID(job.getJobID(), 1), 1);

    TestMessageManager messenger = new TestMessageManager();
    PeerSyncClient syncClient = SyncServiceFactory.getPeerSyncClient(config);
    BSPPeer bspTask = new TestBSPPeer(job, config, taskId, new Counters(), -1L,
        (BSPPeerSyncClient) syncClient, messenger, TaskStatus.State.RUNNING);

    assertNotNull("BSPPeerImpl should not be null.", bspTask);

    LOG.info("Created bsp peer and other parameters");

    @SuppressWarnings("unused")
    FaultTolerantPeerService<Text> service = null;

    bspTask.sync();
    LOG.info("Completed first sync.");

    checkSuperstepMsgCount(syncClient, bspTask, job, 1L, 0L);

    Text txtMessage = new Text("data");
    messenger.addMessage(txtMessage);

    bspTask.sync();

    LOG.info("Completed second sync.");

    checkSuperstepMsgCount(syncClient, bspTask, job, 2L, 1L);

    // Checking the messages for superstep 2 and peer id 1
    String expectedPath = "checkpoint/job_checkpttest_0001/2/1";
    FSDataInputStream in = dfs.open(new Path(expectedPath));

    String className = in.readUTF();
    Text message = (Text) ReflectionUtils.newInstance(Class.forName(className),
        config);
    message.readFields(in);

    assertEquals("data", message.toString());

    dfs.delete(new Path("checkpoint"), true);
  }

  public void testPeerRecovery() throws Exception {
    Configuration config = new Configuration();
    config.set(SyncServiceFactory.SYNC_PEER_CLASS,
        TempSyncClient.class.getName());
    config.set(Constants.FAULT_TOLERANCE_CLASS,
        AsyncRcvdMsgCheckpointImpl.class.getName());
    config.setBoolean(Constants.CHECKPOINT_ENABLED, true);
    int port = BSPNetUtils.getFreePort(12502);
    LOG.info("Got port = " + port);

    config.set(Constants.PEER_HOST, Constants.DEFAULT_PEER_HOST);
    config.setInt(Constants.PEER_PORT, port);

    config.set("bsp.output.dir", "/tmp/hama-test_out");
    config.set("bsp.local.dir", "/tmp/hama-test");

    FileSystem dfs = FileSystem.get(config);
    BSPJob job = new BSPJob(new BSPJobID("checkpttest", 1), "/tmp");
    TaskAttemptID taskId = new TaskAttemptID(new TaskID(job.getJobID(), 1), 1);

    TestMessageManager messenger = new TestMessageManager();
    PeerSyncClient syncClient = SyncServiceFactory.getPeerSyncClient(config);

    Text txtMessage = new Text("data");
    String writeKey = "job_checkpttest_0001/checkpoint/1/";

    Writable[] writableArr = new Writable[2];
    writableArr[0] = new LongWritable(3L);
    writableArr[1] = new LongWritable(5L);
    ArrayWritable arrWritable = new ArrayWritable(LongWritable.class);
    arrWritable.set(writableArr);
    syncClient.storeInformation(writeKey, arrWritable, true, null);

    String writePath = "checkpoint/job_checkpttest_0001/3/1";
    FSDataOutputStream out = dfs.create(new Path(writePath));
    for (int i = 0; i < 5; ++i) {
      out.writeUTF(txtMessage.getClass().getCanonicalName());
      txtMessage.write(out);
    }
    out.close();

    @SuppressWarnings("unused")
    BSPPeer<?, ?, ?, ?, Text> bspTask = new TestBSPPeer(job, config, taskId,
        new Counters(), 3L, (BSPPeerSyncClient) syncClient, messenger,
        TaskStatus.State.RECOVERING);

    BSPMessageBundle<Text> bundleRead = messenger.getLoopbackBundle();
    assertEquals(5, bundleRead.getMessages().size());
    String recoveredMsg = bundleRead.getMessages().get(0).toString();
    assertEquals(recoveredMsg, "data");
    dfs.delete(new Path("checkpoint"), true);
  }

}
