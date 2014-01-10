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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.pipes.Submitter;

/**
 * This protocol is a binary implementation of the Hama Pipes protocol.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class BinaryProtocol<K1, V1, K2, V2, M extends Writable> implements
    DownwardProtocol<K1, V1, K2, V2> {

  protected static final Log LOG = LogFactory.getLog(BinaryProtocol.class);
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket
   */
  protected static final int BUFFER_SIZE = 128 * 1024;
  protected final DataOutputStream outStream;
  /* protected final peer is only needed by the Streaming Protocol */
  protected final BSPPeer<K1, V1, K2, V2, M> peer;

  public final Object hasTaskLock = new Object();
  private boolean hasTask = false;
  private Throwable uplinkException = null;
  public final Object resultLock = new Object();
  private Integer resultInt = null;

  private UplinkReader<K1, V1, K2, V2, M> uplink;
  private Configuration conf;

  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward downward
   * messages are public methods on this object.
   * 
   * @param conf The job's configuration
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

    this.outStream = new DataOutputStream(new BufferedOutputStream(out,
        BUFFER_SIZE));

    this.uplink = new UplinkReader<K1, V1, K2, V2, M>(this, conf, in);
    this.uplink.setName("pipe-uplink-handler");
    this.uplink.start();
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
  public BinaryProtocol(BSPPeer<K1, V1, K2, V2, M> peer, OutputStream out,
      InputStream in) throws IOException {
    this.peer = peer;
    this.conf = peer.getConfiguration();

    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(conf)) {
      out = new TeeOutputStream("downlink.data", out);
    }

    this.outStream = new DataOutputStream(new BufferedOutputStream(out,
        BUFFER_SIZE));

    this.uplink = getUplinkReader(peer, in);
    this.uplink.setName("pipe-uplink-handler");
    this.uplink.start();
  }

  public UplinkReader<K1, V1, K2, V2, M> getUplinkReader(
      BSPPeer<K1, V1, K2, V2, M> peer, InputStream in) throws IOException {
    return new UplinkReader<K1, V1, K2, V2, M>(this, peer, in);
  }

  public void setUplinkException(Throwable e) {
    this.uplinkException = e;
  }

  public boolean isHasTask() {
    return this.hasTask;
  }

  public synchronized void setHasTask(boolean hasTask) {
    this.hasTask = hasTask;
  }

  public synchronized void setResult(int result) {
    this.resultInt = result;
  }

  public DataOutputStream getOutputStream() {
    return this.outStream;
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
    WritableUtils.writeVInt(this.outStream, MessageType.START.code);
    WritableUtils.writeVInt(this.outStream, CURRENT_PROTOCOL_VERSION);
    flush();
    LOG.debug("Sent MessageType.START");
    setBSPJobConf(conf);
  }

  @Override
  public void setBSPJobConf(Configuration conf) throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.SET_BSPJOB_CONF.code);
    List<Entry<String, String>> list = new ArrayList<Entry<String, String>>();
    for (Entry<String, String> entry : conf) {
      list.add(entry);
    }
    WritableUtils.writeVInt(this.outStream, list.size());
    for (Entry<String, String> entry : list) {
      Text.writeString(this.outStream, entry.getKey());
      Text.writeString(this.outStream, entry.getValue());
    }
    flush();
    LOG.debug("Sent MessageType.SET_BSPJOB_CONF including " + list.size()
        + " entries.");
  }

  @Override
  public void setInputTypes(String keyType, String valueType)
      throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.SET_INPUT_TYPES.code);
    Text.writeString(this.outStream, keyType);
    Text.writeString(this.outStream, valueType);
    flush();
    LOG.debug("Sent MessageType.SET_INPUT_TYPES");
  }

  @Override
  public void runSetup() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.RUN_SETUP.code);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_SETUP");
  }

  @Override
  public void runBsp() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.RUN_BSP.code);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_BSP");
  }

  @Override
  public void runCleanup() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.RUN_CLEANUP.code);
    flush();
    setHasTask(true);
    LOG.debug("Sent MessageType.RUN_CLEANUP");
  }

  @Override
  public int getPartition(K1 key, V1 value, int numTasks) throws IOException {

    WritableUtils.writeVInt(this.outStream, MessageType.PARTITION_REQUEST.code);
    writeObject((Writable) key);
    writeObject((Writable) value);
    WritableUtils.writeVInt(this.outStream, numTasks);
    flush();

    LOG.debug("Sent MessageType.PARTITION_REQUEST - key: "
        + ((key.toString().length() < 10) ? key.toString() : key.toString()
            .substring(0, 9) + "...")
        + " value: "
        + ((value.toString().length() < 10) ? value.toString() : value
            .toString().substring(0, 9) + "...") + " numTasks: " + numTasks);

    int resultVal = 0;

    synchronized (this.resultLock) {
      try {
        while (resultInt == null) {
          this.resultLock.wait(); // this call blocks
        }

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
    WritableUtils.writeVInt(this.outStream, MessageType.ABORT.code);
    flush();
    LOG.debug("Sent MessageType.ABORT");
  }

  @Override
  public void flush() throws IOException {
    this.outStream.flush();
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

    // Only send closing message back in Hama Pipes NOT in Hama Streaming
    boolean streamingEnabled = conf.getBoolean("hama.streaming.enabled", false);
    if (!streamingEnabled) {
      endOfInput();
    }
    this.uplink.interrupt();
    this.uplink.join();

    this.uplink.closeConnection();
    this.outStream.close();
  }

  @Override
  public boolean waitForFinish() throws IOException, InterruptedException {
    // LOG.debug("waitForFinish... " + hasTask);
    synchronized (this.hasTaskLock) {

      while (this.hasTask) {
        this.hasTaskLock.wait(); // this call blocks
      }

      // Check if UplinkReader thread has thrown exception
      if (uplinkException != null) {
        throw new InterruptedException(
            StringUtils.stringifyException(uplinkException));
      }
    }
    return hasTask;
  }

  public void endOfInput() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.CLOSE.code);
    flush();
    LOG.debug("Sent close command");
    LOG.debug("Sent MessageType.CLOSE");
  }

  /**
   * Write the given object to the stream. If it is a IntWritable, LongWritable,
   * FloatWritable, DoubleWritable, Text or BytesWritable, write it directly.
   * Otherwise, write it to a buffer and then write the length and data to the
   * stream.
   * 
   * @param obj the object to write
   * @throws IOException
   */
  protected void writeObject(Writable obj) throws IOException {
    // For basic types IntWritable, LongWritable, Text and BytesWritable,
    // encode them directly, so that they end up
    // in C++ as the natural translations.
    if (obj instanceof Text) {
      Text t = (Text) obj;
      int len = t.getLength();
      WritableUtils.writeVInt(this.outStream, len);
      this.outStream.write(t.getBytes(), 0, len);

    } else if (obj instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) obj;
      int len = b.getLength();
      WritableUtils.writeVInt(this.outStream, len);
      this.outStream.write(b.getBytes(), 0, len);

    } else if (obj instanceof IntWritable) {
      WritableUtils.writeVInt(this.outStream, ((IntWritable) obj).get());

    } else if (obj instanceof LongWritable) {
      WritableUtils.writeVLong(this.outStream, ((LongWritable) obj).get());

    } else {
      // Note: FloatWritable and DoubleWritable are written here
      obj.write(this.outStream);
    }
  }
}
