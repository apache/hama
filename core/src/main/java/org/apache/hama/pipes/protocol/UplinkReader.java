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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public class UplinkReader<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
    extends Thread {

  private static final Log LOG = LogFactory.getLog(UplinkReader.class);

  protected DataInputStream inStream;
  private K2 key;
  private V2 value;
  
  private BinaryProtocol<K1, V1, K2, V2> binProtocol;
  private BSPPeer<K1, V1, K2, V2, BytesWritable> peer = null;
  private Configuration conf;
  
  private Map<Integer, Entry<SequenceFile.Reader, Entry<String, String>>> sequenceFileReaders;
  private Map<Integer, Entry<SequenceFile.Writer, Entry<String, String>>> sequenceFileWriters;

  @SuppressWarnings("unchecked")
  public UplinkReader(BinaryProtocol<K1, V1, K2, V2> binaryProtocol,
      Configuration conf, InputStream stream) throws IOException {

    this.binProtocol = binaryProtocol;
    this.conf = conf;

    this.inStream = new DataInputStream(new BufferedInputStream(stream,
        BinaryProtocol.BUFFER_SIZE));

    this.key = (K2) ReflectionUtils.newInstance((Class<? extends K2>) conf
        .getClass("bsp.output.key.class", Object.class), conf);

    this.value = (V2) ReflectionUtils.newInstance((Class<? extends V2>) conf
        .getClass("bsp.output.value.class", Object.class), conf);

    this.sequenceFileReaders = new HashMap<Integer, Entry<SequenceFile.Reader, Entry<String, String>>>();
    this.sequenceFileWriters = new HashMap<Integer, Entry<SequenceFile.Writer, Entry<String, String>>>();
  }

  public UplinkReader(BinaryProtocol<K1, V1, K2, V2> binaryProtocol,
      BSPPeer<K1, V1, K2, V2, BytesWritable> peer, InputStream stream)
      throws IOException {
    this(binaryProtocol, peer.getConfiguration(), stream);
    this.peer = peer;
  }

  private boolean isPeerAvailable() {
    return this.peer != null;
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

        if (cmd == MessageType.WRITE_KEYVALUE.code && isPeerAvailable()) { // INCOMING
          writeKeyValue();
        } else if (cmd == MessageType.READ_KEYVALUE.code && isPeerAvailable()) { // OUTGOING
          readKeyValue();
        } else if (cmd == MessageType.INCREMENT_COUNTER.code
            && isPeerAvailable()) { // INCOMING
          incrementCounter();
        } else if (cmd == MessageType.REGISTER_COUNTER.code
            && isPeerAvailable()) { // INCOMING
          /*
           * Is not used in HAMA -> Hadoop Pipes - maybe for performance, skip
           * transferring group and name each INCREMENT
           */
        } else if (cmd == MessageType.TASK_DONE.code) { // INCOMING
          synchronized (binProtocol.hasTaskLock) {
            binProtocol.setHasTask(false);
            LOG.debug("Got MessageType.TASK_DONE");
            binProtocol.hasTaskLock.notify();
          }
        } else if (cmd == MessageType.DONE.code) { // INCOMING
          LOG.debug("Pipe child done");
          return;
        } else if (cmd == MessageType.SEND_MSG.code && isPeerAvailable()) { // INCOMING
          sendMessage();
        } else if (cmd == MessageType.GET_MSG_COUNT.code && isPeerAvailable()) { // OUTGOING
          getMessageCount();
        } else if (cmd == MessageType.GET_MSG.code && isPeerAvailable()) { // OUTGOING
          getMessage();
        } else if (cmd == MessageType.SYNC.code && isPeerAvailable()) { // INCOMING
          sync();
        } else if (cmd == MessageType.GET_ALL_PEERNAME.code
            && isPeerAvailable()) { // OUTGOING
          getAllPeerNames();
        } else if (cmd == MessageType.GET_PEERNAME.code && isPeerAvailable()) { // OUTGOING
          getPeerName();
        } else if (cmd == MessageType.GET_PEER_INDEX.code && isPeerAvailable()) { // OUTGOING
          getPeerIndex();
        } else if (cmd == MessageType.GET_PEER_COUNT.code && isPeerAvailable()) { // OUTGOING
          getPeerCount();
        } else if (cmd == MessageType.GET_SUPERSTEP_COUNT.code
            && isPeerAvailable()) { // OUTGOING
          getSuperstepCount();
        } else if (cmd == MessageType.REOPEN_INPUT.code && isPeerAvailable()) { // INCOMING
          reopenInput();
        } else if (cmd == MessageType.CLEAR.code && isPeerAvailable()) { // INCOMING
          LOG.debug("Got MessageType.CLEAR");
          peer.clear();
          /* SequenceFileConnector Implementation */
        } else if (cmd == MessageType.SEQFILE_OPEN.code) { // OUTGOING
          seqFileOpen();
        } else if (cmd == MessageType.SEQFILE_READNEXT.code) { // OUTGOING
          seqFileReadNext();
        } else if (cmd == MessageType.SEQFILE_APPEND.code) { // INCOMING
          seqFileAppend();
        } else if (cmd == MessageType.SEQFILE_CLOSE.code) { // OUTGOING
          seqFileClose();
          /* SequenceFileConnector Implementation */
        } else if (cmd == MessageType.PARTITION_RESPONSE.code) { // INCOMING
          partitionResponse();
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
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.GET_SUPERSTEP_COUNT.code);
    WritableUtils.writeVLong(stream, peer.getSuperstepCount());
    binProtocol.flush();

    LOG.debug("Responded MessageType.GET_SUPERSTEP_COUNT - SuperstepCount: "
        + peer.getSuperstepCount());
  }

  public void getPeerCount() throws IOException {
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.GET_PEER_COUNT.code);
    WritableUtils.writeVInt(stream, peer.getNumPeers());
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_PEER_COUNT - NumPeers: "
        + peer.getNumPeers());
  }

  public void getPeerIndex() throws IOException {
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.GET_PEER_INDEX.code);
    WritableUtils.writeVInt(stream, peer.getPeerIndex());
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_PEER_INDEX - PeerIndex: "
        + peer.getPeerIndex());
  }

  public void getPeerName() throws IOException {
    DataOutputStream stream = binProtocol.getStream();
    int id = WritableUtils.readVInt(inStream);
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
    binProtocol.flush();
  }

  public void getAllPeerNames() throws IOException {
    DataOutputStream stream = binProtocol.getStream();
    LOG.debug("Got MessageType.GET_ALL_PEERNAME");
    WritableUtils.writeVInt(stream, MessageType.GET_ALL_PEERNAME.code);
    WritableUtils.writeVInt(stream, peer.getAllPeerNames().length);
    for (String s : peer.getAllPeerNames())
      Text.writeString(stream, s);

    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_ALL_PEERNAME - peerNamesCount: "
        + peer.getAllPeerNames().length);
  }

  public void sync() throws IOException, SyncException, InterruptedException {
    LOG.debug("Got MessageType.SYNC");
    peer.sync(); // this call blocks
  }

  public void getMessage() throws IOException {
    DataOutputStream stream = binProtocol.getStream();
    LOG.debug("Got MessageType.GET_MSG");
    WritableUtils.writeVInt(stream, MessageType.GET_MSG.code);
    BytesWritable msg = peer.getCurrentMessage();
    if (msg != null)
      binProtocol.writeObject(msg);

    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_MSG - Message(BytesWritable) ");// +msg);
  }

  public void getMessageCount() throws IOException {
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.GET_MSG_COUNT.code);
    WritableUtils.writeVInt(stream, peer.getNumCurrentMessages());
    binProtocol.flush();
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
    DataOutputStream stream = binProtocol.getStream();
    boolean nullinput = peer.getConfiguration().get(
        Constants.INPUT_FORMAT_CLASS) == null
        || peer.getConfiguration().get(Constants.INPUT_FORMAT_CLASS)
            .equals("org.apache.hama.bsp.NullInputFormat");

    if (!nullinput) {

      KeyValuePair<K1, V1> pair = peer.readNext();

      WritableUtils.writeVInt(stream, MessageType.READ_KEYVALUE.code);
      if (pair != null) {
        binProtocol.writeObject(new Text(pair.getKey().toString()));
        String valueStr = pair.getValue().toString();
        binProtocol.writeObject(new Text(valueStr));

        LOG.debug("Responded MessageType.READ_KEYVALUE - Key: "
            + pair.getKey()
            + " Value: "
            + ((valueStr.length() < 10) ? valueStr : valueStr.substring(0, 9)
                + "..."));

      } else {
        Text.writeString(stream, "");
        Text.writeString(stream, "");
        LOG.debug("Responded MessageType.READ_KEYVALUE - EMPTY KeyValue Pair");
      }
      binProtocol.flush();

    } else {
      /* TODO */
      /* Send empty Strings to show no KeyValue pair is available */
      WritableUtils.writeVInt(stream, MessageType.READ_KEYVALUE.code);
      Text.writeString(stream, "");
      Text.writeString(stream, "");
      binProtocol.flush();
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

  public void seqFileOpen() throws IOException {
    String path = Text.readString(inStream);
    // option - read = "r" or write = "w"
    String option = Text.readString(inStream);
    // key and value Type stored in the SequenceFile
    String keyType = Text.readString(inStream);
    String valueType = Text.readString(inStream);

    int fileID = -1;

    FileSystem fs = FileSystem.get(conf);
    if (option.equals("r")) {
      SequenceFile.Reader reader;
      try {
        reader = new SequenceFile.Reader(fs, new Path(path), conf);
        fileID = reader.hashCode();
        sequenceFileReaders
            .put(
                fileID,
                new AbstractMap.SimpleEntry<SequenceFile.Reader, Entry<String, String>>(
                    reader, new AbstractMap.SimpleEntry<String, String>(
                        keyType, valueType)));
      } catch (IOException e) {
        fileID = -1;
      }

    } else if (option.equals("w")) {
      SequenceFile.Writer writer;
      try {
        writer = new SequenceFile.Writer(fs, conf, new Path(path), Text.class,
            Text.class);
        fileID = writer.hashCode();
        sequenceFileWriters
            .put(
                fileID,
                new AbstractMap.SimpleEntry<SequenceFile.Writer, Entry<String, String>>(
                    writer, new AbstractMap.SimpleEntry<String, String>(
                        keyType, valueType)));
      } catch (IOException e) {
        fileID = -1;
      }
    }

    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.SEQFILE_OPEN.code);
    WritableUtils.writeVInt(stream, fileID);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEQFILE_OPEN - FileID: " + fileID);
  }

  public void seqFileReadNext() throws IOException, ClassNotFoundException {
    int fileID = WritableUtils.readVInt(inStream);
    // LOG.debug("GOT MessageType.SEQFILE_READNEXT - FileID: " + fileID);

    Class<?> keyType = conf.getClassLoader().loadClass(
        sequenceFileReaders.get(fileID).getValue().getKey());
    Writable key = (Writable) ReflectionUtils.newInstance(keyType, conf);

    Class<?> valueType = conf.getClassLoader().loadClass(
        sequenceFileReaders.get(fileID).getValue().getValue());
    Writable value = (Writable) ReflectionUtils.newInstance(valueType, conf);

    if (sequenceFileReaders.containsKey(fileID))
      sequenceFileReaders.get(fileID).getKey().next(key, value);

    // RESPOND
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.SEQFILE_READNEXT.code);
    try {
      String k = key.toString();
      String v = value.toString();
      Text.writeString(stream, k);
      Text.writeString(stream, v);
      LOG.debug("Responded MessageType.SEQFILE_READNEXT - key: " + k
          + " value: " + ((v.length() < 10) ? v : v.substring(0, 9) + "..."));

    } catch (NullPointerException e) { // key or value is null

      Text.writeString(stream, "");
      Text.writeString(stream, "");
      LOG.debug("Responded MessageType.SEQFILE_READNEXT - EMPTY KeyValue Pair");
    }
    binProtocol.flush();
  }

  public void seqFileAppend() throws IOException {
    int fileID = WritableUtils.readVInt(inStream);
    String keyStr = Text.readString(inStream);
    String valueStr = Text.readString(inStream);

    boolean result = false;
    if (sequenceFileWriters.containsKey(fileID)) {
      sequenceFileWriters.get(fileID).getKey()
          .append(new Text(keyStr), new Text(valueStr));
      result = true;
    }

    // RESPOND
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.SEQFILE_APPEND.code);
    WritableUtils.writeVInt(stream, result ? 1 : 0);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEQFILE_APPEND - Result: " + result);
  }

  public void seqFileClose() throws IOException {
    int fileID = WritableUtils.readVInt(inStream);

    boolean result = false;

    if (sequenceFileReaders.containsKey(fileID)) {
      sequenceFileReaders.get(fileID).getKey().close();
      result = true;
    } else if (sequenceFileWriters.containsKey(fileID)) {
      sequenceFileWriters.get(fileID).getKey().close();
      result = true;
    }

    // RESPOND
    DataOutputStream stream = binProtocol.getStream();
    WritableUtils.writeVInt(stream, MessageType.SEQFILE_CLOSE.code);
    WritableUtils.writeVInt(stream, result ? 1 : 0);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEQFILE_CLOSE - Result: " + result);
  }

  public void partitionResponse() throws IOException {
    int partResponse = WritableUtils.readVInt(inStream);
    synchronized (binProtocol.resultLock) {
      binProtocol.setResult(partResponse);
      LOG.debug("Received MessageType.PARTITION_RESPONSE - Result: "
          + partResponse);
      binProtocol.resultLock.notify();
    }
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
