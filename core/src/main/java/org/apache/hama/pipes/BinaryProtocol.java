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

package org.apache.hama.pipes;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

/**
 * This protocol is a binary implementation of the Hama Pipes protocol.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class BinaryProtocol<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
    implements DownwardProtocol<K1, V1> {

  protected static final Log LOG = LogFactory.getLog(BinaryProtocol.class
      .getName());
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket
   */
  private static final int BUFFER_SIZE = 128 * 1024;

  protected final DataOutputStream stream;
  protected final DataOutputBuffer buffer = new DataOutputBuffer();

  private UplinkReaderThread uplink;

  private boolean hasTask = false;
  protected final BSPPeer<K1, V1, K2, V2, BytesWritable> peer;

  /**
   * The integer codes to represent the different messages. These must match the
   * C++ codes or massive confusion will result.
   */
  protected static enum MessageType {
    START(0), SET_BSPJOB_CONF(1), SET_INPUT_TYPES(2), RUN_SETUP(3), RUN_BSP(4),
    RUN_CLEANUP(5), READ_KEYVALUE(6), WRITE_KEYVALUE(7), GET_MSG(8),
    GET_MSG_COUNT(9), SEND_MSG(10), SYNC(11), GET_ALL_PEERNAME(12),
    GET_PEERNAME(13), GET_PEER_INDEX(14), GET_PEER_COUNT(15),
    GET_SUPERSTEP_COUNT(16), REOPEN_INPUT(17), CLEAR(18), CLOSE(19), ABORT(20),
    DONE(21), TASK_DONE(22), REGISTER_COUNTER(23), INCREMENT_COUNTER(24), LOG(
        25);

    final int code;

    MessageType(int code) {
      this.code = code;
    }
  }

  protected class UplinkReaderThread extends Thread {

    protected DataInputStream inStream;
    protected K2 key;
    protected V2 value;
    protected BSPPeer<K1, V1, K2, V2, BytesWritable> peer;

    @SuppressWarnings("unchecked")
    public UplinkReaderThread(BSPPeer<K1, V1, K2, V2, BytesWritable> peer,
        InputStream stream) throws IOException {

      inStream = new DataInputStream(new BufferedInputStream(stream,
          BUFFER_SIZE));

      this.peer = peer;
      this.key = ReflectionUtils.newInstance((Class<? extends K2>) peer
          .getConfiguration().getClass("bsp.output.key.class", Object.class),
          peer.getConfiguration());

      this.value = ReflectionUtils.newInstance((Class<? extends V2>) peer
          .getConfiguration().getClass("bsp.output.value.class", Object.class),
          peer.getConfiguration());
    }

    public void closeConnection() throws IOException {
      inStream.close();
    }

    @Override
    public void run() {
      while (true) {
        try {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }

          int cmd = readCommand();
          if (cmd == -1)
            continue;
          LOG.debug("Handling uplink command " + cmd);

          if (cmd == MessageType.WRITE_KEYVALUE.code) { // INCOMING
            writeKeyValue();
          } else if (cmd == MessageType.READ_KEYVALUE.code) { // OUTGOING
            readKeyValue();
          } else if (cmd == MessageType.INCREMENT_COUNTER.code) { // INCOMING
            incrementCounter();
          } else if (cmd == MessageType.REGISTER_COUNTER.code) { // INCOMING
            /*
             * Is not used in HAMA -> Hadoop Pipes - maybe for performance, skip
             * transferring group and name each INCREMENT
             */
          } else if (cmd == MessageType.TASK_DONE.code) { // INCOMING
            LOG.debug("Got MessageType.TASK_DONE");
            hasTask = false;
          } else if (cmd == MessageType.DONE.code) { // INCOMING
            LOG.debug("Pipe child done");
            return;
          } else if (cmd == MessageType.SEND_MSG.code) { // INCOMING
            sendMessage();
          } else if (cmd == MessageType.GET_MSG_COUNT.code) { // OUTGOING
            getMessageCount();
          } else if (cmd == MessageType.GET_MSG.code) { // OUTGOING
            getMessage();
          } else if (cmd == MessageType.SYNC.code) { // INCOMING
            sync();
          } else if (cmd == MessageType.GET_ALL_PEERNAME.code) { // OUTGOING
            getAllPeerNames();
          } else if (cmd == MessageType.GET_PEERNAME.code) { // OUTGOING
            getPeerName();
          } else if (cmd == MessageType.GET_PEER_INDEX.code) { // OUTGOING
            getPeerIndex();
          } else if (cmd == MessageType.GET_PEER_COUNT.code) { // OUTGOING
            getPeerCount();
          } else if (cmd == MessageType.GET_SUPERSTEP_COUNT.code) { // OUTGOING
            getSuperstepCount();
          } else if (cmd == MessageType.REOPEN_INPUT.code) { // INCOMING
            reopenInput();
          } else if (cmd == MessageType.CLEAR.code) { // INCOMING
            LOG.debug("Got MessageType.CLEAR");
            peer.clear();
          } else {
            throw new IOException("Bad command code: " + cmd);
          }
        } catch (InterruptedException e) {
          return;
        } catch (Throwable e) {
          onError(e);
          throw new RuntimeException(e);
        }
      }
    }

    protected void onError(Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
    }

    public int readCommand() throws IOException {
      return WritableUtils.readVInt(inStream);
    }

    public void reopenInput() throws IOException {
      LOG.debug("Got MessageType.REOPEN_INPUT");
      peer.reopenInput();
    }

    public void getSuperstepCount() throws IOException {
      WritableUtils.writeVInt(stream, MessageType.GET_SUPERSTEP_COUNT.code);
      WritableUtils.writeVLong(stream, peer.getSuperstepCount());
      flush();
      LOG.debug("Responded MessageType.GET_SUPERSTEP_COUNT - SuperstepCount: "
          + peer.getSuperstepCount());
    }

    public void getPeerCount() throws IOException {
      WritableUtils.writeVInt(stream, MessageType.GET_PEER_COUNT.code);
      WritableUtils.writeVInt(stream, peer.getNumPeers());
      flush();
      LOG.debug("Responded MessageType.GET_PEER_COUNT - NumPeers: "
          + peer.getNumPeers());
    }

    public void getPeerIndex() throws IOException {
      WritableUtils.writeVInt(stream, MessageType.GET_PEER_INDEX.code);
      WritableUtils.writeVInt(stream, peer.getPeerIndex());
      flush();
      LOG.debug("Responded MessageType.GET_PEER_INDEX - PeerIndex: "
          + peer.getPeerIndex());
    }

    public void getPeerName() throws IOException {
      int id = readCommand();
      LOG.debug("Got MessageType.GET_PEERNAME id: " + id);

      WritableUtils.writeVInt(stream, MessageType.GET_PEERNAME.code);
      if (id == -1) { // -1 indicates get own PeerName
        Text.writeString(stream, peer.getPeerName());
        LOG.debug("Responded MessageType.GET_PEERNAME - Get Own PeerName: "
            + peer.getPeerName());

      } else if ((id < -1) || (id >= peer.getNumPeers())) {
        // if no PeerName for this index is found write emptyString
        Text.writeString(stream, "");
        LOG.debug("Responded MessageType.GET_PEERNAME - Empty PeerName!");

      } else {
        Text.writeString(stream, peer.getPeerName(id));
        LOG.debug("Responded MessageType.GET_PEERNAME - PeerName: "
            + peer.getPeerName(id));
      }
      flush();
    }

    public void getAllPeerNames() throws IOException {
      LOG.debug("Got MessageType.GET_ALL_PEERNAME");
      WritableUtils.writeVInt(stream, MessageType.GET_ALL_PEERNAME.code);
      WritableUtils.writeVInt(stream, peer.getAllPeerNames().length);
      for (String s : peer.getAllPeerNames())
        Text.writeString(stream, s);

      flush();
      LOG.debug("Responded MessageType.GET_ALL_PEERNAME - peerNamesCount: "
          + peer.getAllPeerNames().length);
    }

    public void sync() throws IOException, SyncException, InterruptedException {
      LOG.debug("Got MessageType.SYNC");
      peer.sync(); // this call blocks
    }

    public void getMessage() throws IOException {
      LOG.debug("Got MessageType.GET_MSG");
      WritableUtils.writeVInt(stream, MessageType.GET_MSG.code);
      BytesWritable msg = peer.getCurrentMessage();
      if (msg != null)
        writeObject(msg);

      flush();
      LOG.debug("Responded MessageType.GET_MSG - Message(BytesWritable) ");// +msg);
    }

    public void getMessageCount() throws IOException {
      WritableUtils.writeVInt(stream, MessageType.GET_MSG_COUNT.code);
      WritableUtils.writeVInt(stream, peer.getNumCurrentMessages());
      flush();
      LOG.debug("Responded MessageType.GET_MSG_COUNT - Count: "
          + peer.getNumCurrentMessages());
    }

    public void sendMessage() throws IOException {
      String peerName = Text.readString(inStream);
      BytesWritable msg = new BytesWritable();
      readObject(msg);
      LOG.debug("Got MessageType.SEND_MSG to peerName: " + peerName);
      peer.send(peerName, msg);
    }

    public void incrementCounter() throws IOException {
      // int id = WritableUtils.readVInt(inStream);
      String group = Text.readString(inStream);
      String name = Text.readString(inStream);
      long amount = WritableUtils.readVLong(inStream);
      peer.incrementCounter(name, group, amount);
    }

    public void readKeyValue() throws IOException {
      boolean nullinput = peer.getConfiguration().get("bsp.input.format.class") == null
          || peer.getConfiguration().get("bsp.input.format.class")
              .equals("org.apache.hama.bsp.NullInputFormat");

      if (!nullinput) {

        KeyValuePair<K1, V1> pair = peer.readNext();

        WritableUtils.writeVInt(stream, MessageType.READ_KEYVALUE.code);
        if (pair != null) {
          writeObject(pair.getKey());
          writeObject(pair.getValue());

          LOG.debug("Responded MessageType.READ_KEYVALUE - Key: "
              + pair.getKey() + " Value: " + pair.getValue());

        } else {
          Text.writeString(stream, "");
          Text.writeString(stream, "");
          LOG.debug("Responded MessageType.READ_KEYVALUE - EMPTY KeyValue Pair");
        }
        flush();

      } else {
        /* TODO */
        /* Send empty Strings to show no KeyValue pair is available */
        WritableUtils.writeVInt(stream, MessageType.READ_KEYVALUE.code);
        Text.writeString(stream, "");
        Text.writeString(stream, "");
        flush();
        LOG.debug("Responded MessageType.READ_KEYVALUE - EMPTY KeyValue Pair");
      }
    }

    public void writeKeyValue() throws IOException {
      readObject(key); // string or binary only
      readObject(value); // string or binary only
      if (LOG.isDebugEnabled())
        LOG.debug("Got MessageType.WRITE_KEYVALUE - Key: " + key + " Value: "
            + value);
      peer.write(key, value);
    }

    protected void readObject(Writable obj) throws IOException {
      int numBytes = readCommand();
      byte[] buffer;
      // For BytesWritable and Text, use the specified length to set the length
      // this causes the "obvious" translations to work. So that if you emit
      // a string "abc" from C++, it shows up as "abc".
      if (obj instanceof BytesWritable) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((BytesWritable) obj).set(buffer, 0, numBytes);
      } else if (obj instanceof Text) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((Text) obj).set(buffer);
      } else if (obj instanceof NullWritable) {
        throw new IOException(
            "Cannot read data into NullWritable! Check OutputClasses!");
      } else {
        /* TODO */
        /* IntWritable, DoubleWritable */
        throw new IOException(
            "Hama Pipes does only support Text as Key/Value output!");
        // obj.readFields(inStream);
      }
    }
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

  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward downward
   * messages are public methods on this object.
   * 
   * @param sock The socket to communicate on.
   * @param handler The handler for the received messages.
   * @param key The object to read keys into.
   * @param value The object to read values into.
   * @param jobConfig The job's configuration
   * @throws IOException
   */
  public BinaryProtocol(BSPPeer<K1, V1, K2, V2, BytesWritable> peer,
      OutputStream out, InputStream in) throws IOException {
    this.peer = peer;
    OutputStream raw = out;

    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(peer.getConfiguration())) {
      raw = new TeeOutputStream("downlink.data", raw);
    }
    stream = new DataOutputStream(new BufferedOutputStream(raw, BUFFER_SIZE));
    uplink = getUplinkReader(peer, in);

    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  public UplinkReaderThread getUplinkReader(
      BSPPeer<K1, V1, K2, V2, BytesWritable> peer, InputStream sock)
      throws IOException {
    return new UplinkReaderThread(peer, sock);
  }

  @Override
  public boolean waitForFinish() throws IOException, InterruptedException {
    // LOG.debug("waitForFinish... "+hasTask);
    while (hasTask) {
      try {
        Thread.sleep(100);
        // LOG.debug("waitForFinish... "+hasTask);
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    return hasTask;
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
  public void start() throws IOException {
    LOG.debug("starting downlink");
    WritableUtils.writeVInt(stream, MessageType.START.code);
    WritableUtils.writeVInt(stream, CURRENT_PROTOCOL_VERSION);
    flush();
    LOG.debug("Sent MessageType.START");
    setBSPJob(peer.getConfiguration());
  }

  public void setBSPJob(Configuration conf) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_BSPJOB_CONF.code);
    List<String> list = new ArrayList<String>();
    for (Map.Entry<String, String> itm : conf) {
      list.add(itm.getKey());
      list.add(itm.getValue());
    }
    WritableUtils.writeVInt(stream, list.size());
    for (String entry : list) {
      Text.writeString(stream, entry);
    }
    flush();
    LOG.debug("Sent MessageType.SET_BSPJOB_CONF");
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
    hasTask = true;
    LOG.debug("Sent MessageType.RUN_SETUP");
  }

  @Override
  public void runBsp(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_BSP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    hasTask = true;
    LOG.debug("Sent MessageType.RUN_BSP");
  }

  @Override
  public void runCleanup(boolean pipedInput, boolean pipedOutput)
      throws IOException {

    WritableUtils.writeVInt(stream, MessageType.RUN_CLEANUP.code);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
    flush();
    hasTask = true;
    LOG.debug("Sent MessageType.RUN_CLEANUP");
  }

  public void endOfInput() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.CLOSE.code);
    flush();
    LOG.debug("Sent close command");
    LOG.debug("Sent MessageType.CLOSE");
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
