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

package org.apache.hama.pipes.protocol;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.pipes.Submitter;
import org.apache.hama.pipes.protocol.UplinkReader;

/**
 * This protocol is a binary implementation of the Hama Pipes protocol.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class BinaryProtocol<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
    implements DownwardProtocol<K1, V1, K2, V2> {

  protected static final Log LOG = LogFactory.getLog(BinaryProtocol.class
      .getName());
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket
   */
  protected static final int BUFFER_SIZE = 128 * 1024;

  protected final DataOutputStream stream;
  protected final DataOutputBuffer buffer = new DataOutputBuffer();

  private UplinkReader<K1, V1, K2, V2> uplink;

  public final Object hasTaskLock = new Object();
  private boolean hasTask = false;
  public final Object resultLock = new Object();
  private Integer resultInt = null;

  /* Protected final peer is only needed by the Streaming Protocol */
  protected final BSPPeer<K1, V1, K2, V2, BytesWritable> peer;
  private Configuration conf;

  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward downward
   * messages are public methods on this object.
   * 
   * @param jobConfig The job's configuration
   * @param out The output stream to communicate on.
   * @param in The input stream to communicate on.
   * @throws IOException
   */
  public BinaryProtocol(Configuration conf, OutputStream out, InputStream in)
      throws IOException {
    this.conf = conf;
    this.peer = null;

    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(conf)) {
      out = new TeeOutputStream("downlink.data", out);
    }
    stream = new DataOutputStream(new BufferedOutputStream(out, BUFFER_SIZE));
    uplink = new UplinkReader<K1, V1, K2, V2>(this, conf, in);

    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward downward
   * messages are public methods on this object.
   * 
   * @param peer the current peer including the task's configuration
   * @param out The output stream to communicate on.
   * @param in The input stream to communicate on.
   * @throws IOException
   */
  public BinaryProtocol(BSPPeer<K1, V1, K2, V2, BytesWritable> peer,
      OutputStream out, InputStream in) throws IOException {
    this.peer = peer;
    this.conf = peer.getConfiguration();

    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(conf)) {
      out = new TeeOutputStream("downlink.data", out);
    }
    stream = new DataOutputStream(new BufferedOutputStream(out, BUFFER_SIZE));
    uplink = getUplinkReader(peer, in);

    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  public UplinkReader<K1, V1, K2, V2> getUplinkReader(
      BSPPeer<K1, V1, K2, V2, BytesWritable> peer, InputStream in) throws IOException {
    return new UplinkReader<K1, V1, K2, V2>(this, peer, in);
  }

  public boolean isHasTask() {
    return hasTask;
  }

  public synchronized void setHasTask(boolean hasTask) {
    this.hasTask = hasTask;
  }

  public synchronized void setResult(int result) {
    this.resultInt = result;
  }

  public DataOutputStream getStream() {
    return stream;
  }

  /**
   * An output stream that will save a copy of the data into a file.
   */
  private static class TeeOutputStream extends FilterOutputStream {
    private OutputStream file;

    TeeOutputStream(String filename, OutputStream base) throws IOException {
      super(base);
      file = new FileOutputStream(filename);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      file.write(b, off, len);
      out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      file.write(b);
      out.write(b);
    }

    @Override
    public void flush() throws IOException {
      file.flush();
      out.flush();
    }

    @Override
    public void close() throws IOException {
      flush();
      file.close();
      out.close();
    }
  }

  /* ************************************************************ */
  /* Implementation of DownwardProtocol<K1, V1, K2, V2> */
  /* ************************************************************ */

  @Override
  public void start() throws IOException {
    LOG.debug("starting downlink");
    WritableUtils.writeVInt(stream, MessageType.START.code);
    WritableUtils.writeVInt(stream, CURRENT_PROTOCOL_VERSION);
    flush();
    LOG.debug("Sent MessageType.START");
    setBSPJobConf(conf);
  }

  @Override
  public void setBSPJobConf(Configuration conf) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_BSPJOB_CONF.code);
    List<Entry<String, String>> list = new ArrayList<Entry<String, String>>();
    for (Entry<String, String> entry : conf) {
      list.add(entry);
    }
    WritableUtils.writeVInt(stream, list.size());
    for (Entry<String, String> entry : list) {
      Text.writeString(stream, entry.getKey());
      Text.writeString(stream, entry.getValue());
    }
    flush();
    LOG.debug("Sent MessageType.SET_BSPJOB_CONF including " + list.size()
        + " entries.");
  }

  @Override
  public void setInputTypes(String keyType, String valueType)
      throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_INPUT_TYPES.code);
    Text.writeString(stream, keyType);
    Text.writeString(stream, valueType);
    flush();
    LOG.debug("Sent MessageType.SET_INPUT_TYPES");
  }

  @Override
  public void runSetup(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_SETUP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_SETUP");
  }

  @Override
  public void runBsp(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_BSP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_BSP");
  }

  @Override
  public void runCleanup(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_CLEANUP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_CLEANUP");
  }

  @Override
  public int getPartition(String key, String value, int numTasks)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.PARTITION_REQUEST.code);
    Text.writeString(stream, key);
    Text.writeString(stream, value);
    WritableUtils.writeVInt(stream, numTasks);
    flush();
    LOG.debug("Sent MessageType.PARTITION_REQUEST - key: " + key + " value: "
        + value.substring(0, 10) + "..." + " numTasks: " + numTasks);

    int resultVal = 0;

    synchronized (resultLock) {
      try {
        while (resultInt == null)
          resultLock.wait();

        resultVal = resultInt;
        resultInt = null;

      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }
    return resultVal;
  }

  @Override
  public void abort() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.ABORT.code);
    flush();
    LOG.debug("Sent MessageType.ABORT");
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  /**
   * Close the connection and shutdown the handler thread.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void close() throws IOException, InterruptedException {
    // runCleanup(pipedInput,pipedOutput);
    LOG.debug("closing connection");
    endOfInput();

    uplink.interrupt();
    uplink.join();

    uplink.closeConnection();
    stream.close();
  }

  @Override
  public boolean waitForFinish() throws IOException, InterruptedException {
    // LOG.debug("waitForFinish... "+hasTask);
    synchronized (hasTaskLock) {
      try {
        while (hasTask)
          hasTaskLock.wait();

      } catch (InterruptedException e) {
        LOG.error(e);
      }
    }

    return hasTask;
  }

  public void endOfInput() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.CLOSE.code);
    flush();
    LOG.debug("Sent close command");
    LOG.debug("Sent MessageType.CLOSE");
  }

  /**
   * Write the given object to the stream. If it is a Text or BytesWritable,
   * write it directly. Otherwise, write it to a buffer and then write the
   * length and data to the stream.
   * 
   * @param obj the object to write
   * @throws IOException
   */
  protected void writeObject(Writable obj) throws IOException {
    // For Text and BytesWritable, encode them directly, so that they end up
    // in C++ as the natural translations.
    if (obj instanceof Text) {
      Text t = (Text) obj;
      int len = t.getLength();
      WritableUtils.writeVInt(stream, len);
      stream.write(t.getBytes(), 0, len);
    } else if (obj instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) obj;
      int len = b.getLength();
      WritableUtils.writeVInt(stream, len);
      stream.write(b.getBytes(), 0, len);
    } else {
      buffer.reset();
      obj.write(buffer);
      int length = buffer.getLength();
      WritableUtils.writeVInt(stream, length);
      stream.write(buffer.getData(), 0, length);
    }
  }

}
