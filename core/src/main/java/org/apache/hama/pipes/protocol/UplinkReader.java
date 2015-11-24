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
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;

public class UplinkReader<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M extends Writable>
    extends Thread {

  private static final Log LOG = LogFactory.getLog(UplinkReader.class);

  private BinaryProtocol<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> binProtocol;
  private BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> peer = null;
  private Configuration conf;
  private FileSystem fs;

  protected DataInputStream inStream;
  protected DataOutputStream outStream;

  private Map<Integer, Entry<SequenceFile.Reader, Entry<Writable, Writable>>> sequenceFileReaders;
  private Map<Integer, Entry<SequenceFile.Writer, Entry<Writable, Writable>>> sequenceFileWriters;
  private Set<String> sequenceFileWriterPaths;

  public UplinkReader(
      BinaryProtocol<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> binaryProtocol,
      Configuration conf, InputStream stream) throws IOException {

    this.binProtocol = binaryProtocol;
    this.conf = conf;
    this.fs = FileSystem.get(conf);

    this.inStream = new DataInputStream(new BufferedInputStream(stream,
        BinaryProtocol.BUFFER_SIZE));

    this.outStream = binProtocol.getOutputStream();

    this.sequenceFileReaders = new HashMap<Integer, Entry<SequenceFile.Reader, Entry<Writable, Writable>>>();
    this.sequenceFileWriters = new HashMap<Integer, Entry<SequenceFile.Writer, Entry<Writable, Writable>>>();
    this.sequenceFileWriterPaths = new HashSet<String>();
  }

  public UplinkReader(
      BinaryProtocol<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> binaryProtocol,
      BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, M> peer, InputStream stream)
      throws IOException {
    this(binaryProtocol, peer.getConfiguration(), stream);
    this.peer = peer;
  }

  private boolean isPeerAvailable() {
    return this.peer != null;
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }

        int cmd = readCommand();
        LOG.debug("Handling uplink command: " + cmd);
        // MessageType.values()[cmd] may cause NullPointerException (bad
        // command)

        if (cmd == -1) {
          continue;
        } else if (cmd == MessageType.WRITE_KEYVALUE.code && isPeerAvailable()) { // INCOMING
          writeKeyValue();
        } else if (cmd == MessageType.READ_KEYVALUE.code && isPeerAvailable()) { // OUTGOING
          readKeyValue();
        } else if (cmd == MessageType.INCREMENT_COUNTER.code
            && isPeerAvailable()) { // INCOMING
          incrementCounter();
        } else if (cmd == MessageType.REGISTER_COUNTER.code
            && isPeerAvailable()) { // INCOMING
          /*
           * Is not used in Hama. Hadoop Pipes uses it - maybe for performance
           * issue, skip transferring group and name each INCREMENT
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
          clear();

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
          throw new Exception("Bad command code: " + cmd);
        }

      } catch (InterruptedException e) {
        onError(e);
        return;
      } catch (Throwable e) {
        onError(e);
        throw new RuntimeException(e);
      }
    }
  }

  // onError is overwritten by StreamingProtocol in Hama Streaming
  protected void onError(Throwable e) {
    LOG.error(StringUtils.stringifyException(e));

    // notify binaryProtocol and set Exception
    synchronized (binProtocol.hasTaskLock) {
      binProtocol.setUplinkException(e);
      binProtocol.setHasTask(false);
      binProtocol.hasTaskLock.notify();
    }
  }

  // readCommand is overwritten by StreamingProtocol in Hama Streaming
  protected int readCommand() throws IOException {
    return WritableUtils.readVInt(this.inStream);
  }

  public void closeConnection() throws IOException {
    // close input stream
    this.inStream.close();

    // close open SequenceFileReaders
    for (int fileID : this.sequenceFileReaders.keySet()) {
      LOG.debug("close SequenceFileReader: " + fileID);
      this.sequenceFileReaders.get(fileID).getKey().close();
    }

    // close open SequenceFileWriters
    for (int fileID : this.sequenceFileWriters.keySet()) {
      LOG.debug("close SequenceFileWriter: " + fileID);
      this.sequenceFileWriters.get(fileID).getKey().close();
    }
  }

  public void reopenInput() throws IOException {
    LOG.debug("Got MessageType.REOPEN_INPUT");

    peer.reopenInput();

    WritableUtils.writeVInt(this.outStream, MessageType.REOPEN_INPUT.code);
    binProtocol.flush();
    LOG.debug("Responded MessageType.REOPEN_INPUT");
  }

  public void clear() throws IOException {
    LOG.debug("Got MessageType.CLEAR");

    peer.clear();

    WritableUtils.writeVInt(this.outStream, MessageType.CLEAR.code);
    binProtocol.flush();
    LOG.debug("Responded MessageType.CLEAR");
  }

  public void getSuperstepCount() throws IOException {
    WritableUtils.writeVInt(this.outStream,
        MessageType.GET_SUPERSTEP_COUNT.code);
    WritableUtils.writeVLong(this.outStream, peer.getSuperstepCount());
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_SUPERSTEP_COUNT - SuperstepCount: "
        + peer.getSuperstepCount());
  }

  public void getPeerCount() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.GET_PEER_COUNT.code);
    WritableUtils.writeVInt(this.outStream, peer.getNumPeers());
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_PEER_COUNT - NumPeers: "
        + peer.getNumPeers());
  }

  public void getPeerIndex() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.GET_PEER_INDEX.code);
    WritableUtils.writeVInt(this.outStream, peer.getPeerIndex());
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_PEER_INDEX - PeerIndex: "
        + peer.getPeerIndex());
  }

  public void getPeerName() throws IOException {
    int id = WritableUtils.readVInt(this.inStream);
    LOG.debug("Got MessageType.GET_PEERNAME id: " + id);

    WritableUtils.writeVInt(this.outStream, MessageType.GET_PEERNAME.code);
    if (id == -1) { // -1 indicates get own PeerName
      Text.writeString(this.outStream, peer.getPeerName());
      LOG.debug("Responded MessageType.GET_PEERNAME - Get Own PeerName: "
          + peer.getPeerName());

    } else if ((id < -1) || (id >= peer.getNumPeers())) {
      // if no PeerName for this index is found write emptyString
      Text.writeString(this.outStream, "");
      LOG.debug("Responded MessageType.GET_PEERNAME - Empty PeerName!");

    } else {
      Text.writeString(this.outStream, peer.getPeerName(id));
      LOG.debug("Responded MessageType.GET_PEERNAME - PeerName: "
          + peer.getPeerName(id));
    }
    binProtocol.flush();
  }

  public void getAllPeerNames() throws IOException {
    LOG.debug("Got MessageType.GET_ALL_PEERNAME");
    String[] peerNames = peer.getAllPeerNames();
    WritableUtils.writeVInt(this.outStream, MessageType.GET_ALL_PEERNAME.code);
    WritableUtils.writeVInt(this.outStream, peerNames.length);
    for (String s : peerNames) {
      Text.writeString(this.outStream, s);
    }
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_ALL_PEERNAME - peerNamesCount: "
        + peerNames.length);
  }

  public void sync() throws IOException, SyncException, InterruptedException {
    LOG.debug("Got MessageType.SYNC");

    peer.sync(); // this call blocks

    WritableUtils.writeVInt(this.outStream, MessageType.SYNC.code);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SYNC");
  }

  public void getMessage() throws IOException {
    LOG.debug("Got MessageType.GET_MSG");
    Writable message = peer.getCurrentMessage();
    if (message != null) {
      WritableUtils.writeVInt(this.outStream, MessageType.GET_MSG.code);
      binProtocol.writeObject(message);
      LOG.debug("Responded MessageType.GET_MSG - Message: "
          + ((message.toString().length() < 10) ? message.toString() : message
              .toString().substring(0, 9) + "..."));
    } else {
      WritableUtils.writeVInt(this.outStream, MessageType.END_OF_DATA.code);
      LOG.debug("Responded MessageType.END_OF_DATA");
    }
    binProtocol.flush();
  }

  public void getMessageCount() throws IOException {
    WritableUtils.writeVInt(this.outStream, MessageType.GET_MSG_COUNT.code);
    WritableUtils.writeVInt(this.outStream, peer.getNumCurrentMessages());
    binProtocol.flush();
    LOG.debug("Responded MessageType.GET_MSG_COUNT - Count: "
        + peer.getNumCurrentMessages());
  }

  public void incrementCounter() throws IOException {
    LOG.debug("Got MessageType.INCREMENT_COUNTER");

    String group = Text.readString(this.inStream);
    String name = Text.readString(this.inStream);
    long amount = WritableUtils.readVLong(this.inStream);

    LOG.debug("Got MessageType.INCREMENT_COUNTER group: " + group + " name: "
        + name + " amount: " + amount);

    peer.incrementCounter(group, name, amount);

    WritableUtils.writeVInt(this.outStream, MessageType.INCREMENT_COUNTER.code);
    binProtocol.flush();
    LOG.debug("Responded MessageType.INCREMENT_COUNTER");
  }

  @SuppressWarnings("unchecked")
  public void sendMessage() throws IOException, InstantiationException,
      IllegalAccessException {
    String peerName = Text.readString(this.inStream);

    M message = (M) ReflectionUtils.newInstance((Class<? extends M>) conf
        .getClass(Constants.MESSAGE_CLASS, BytesWritable.class), conf);

    LOG.debug("Got MessageType.SEND_MSG peerName: " + peerName
        + " messageClass: " + message.getClass().getName());

    readObject(message);

    peer.send(peerName, message);

    WritableUtils.writeVInt(this.outStream, MessageType.SEND_MSG.code);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEND_MSG");

    LOG.debug("Sent message to peerName: "
        + peerName
        + " messageClass: "
        + message.getClass().getName()
        + " Message: "
        + ((message.toString().length() < 10) ? message.toString() : message
            .toString().substring(0, 9) + "..."));
  }

  public void readKeyValue() throws IOException {

    boolean nullinput = peer.getConfiguration().get(
        Constants.INPUT_FORMAT_CLASS) == null
        || peer.getConfiguration().get(Constants.INPUT_FORMAT_CLASS)
            .equals("org.apache.hama.bsp.NullInputFormat");

    if (!nullinput) {

      KeyValuePair<KEYIN, VALUEIN> pair = peer.readNext();

      if (pair != null) {
        WritableUtils.writeVInt(this.outStream, MessageType.READ_KEYVALUE.code);
        binProtocol.writeObject((Writable) pair.getKey());
        binProtocol.writeObject((Writable) pair.getValue());

        LOG.debug("Responded MessageType.READ_KEYVALUE -"
            + " Key: "
            + ((pair.getKey().toString().length() < 10) ? pair.getKey()
                .toString() : pair.getKey().toString().substring(0, 9) + "...")
            + " Value: "
            + ((pair.getValue().toString().length() < 10) ? pair.getValue()
                .toString() : pair.getValue().toString().substring(0, 9)
                + "..."));

      } else {
        WritableUtils.writeVInt(this.outStream, MessageType.END_OF_DATA.code);
        LOG.debug("Responded MessageType.READ_KEYVALUE - END_OF_DATA");
      }
      binProtocol.flush();

    } else {
      WritableUtils.writeVInt(this.outStream, MessageType.END_OF_DATA.code);
      binProtocol.flush();
      LOG.debug("Responded MessageType.READ_KEYVALUE - END_OF_DATA");
    }
  }

  @SuppressWarnings("unchecked")
  public void writeKeyValue() throws IOException {

    KEYOUT keyOut = (KEYOUT) ReflectionUtils.newInstance(
        (Class<? extends KEYOUT>) conf.getClass("bsp.output.key.class",
            Object.class), conf);

    VALUEOUT valueOut = (VALUEOUT) ReflectionUtils.newInstance(
        (Class<? extends VALUEOUT>) conf.getClass("bsp.output.value.class",
            Object.class), conf);

    LOG.debug("Got MessageType.WRITE_KEYVALUE keyOutClass: "
        + keyOut.getClass().getName() + " valueOutClass: "
        + valueOut.getClass().getName());

    readObject((Writable) keyOut);
    readObject((Writable) valueOut);

    peer.write(keyOut, valueOut);

    WritableUtils.writeVInt(this.outStream, MessageType.WRITE_KEYVALUE.code);
    binProtocol.flush();
    LOG.debug("Responded MessageType.WRITE_KEYVALUE");

    LOG.debug("Done MessageType.WRITE_KEYVALUE -"
        + " Key: "
        + ((keyOut.toString().length() < 10) ? keyOut.toString() : keyOut
            .toString().substring(0, 9) + "...")
        + " Value: "
        + ((valueOut.toString().length() < 10) ? valueOut.toString() : valueOut
            .toString().substring(0, 9) + "..."));
  }

  public void seqFileOpen() throws IOException {
    String path = Text.readString(this.inStream);
    // option - read = "r" or write = "w"
    String option = Text.readString(this.inStream);
    // key and value class stored in the SequenceFile
    String keyClass = Text.readString(this.inStream);
    String valueClass = Text.readString(this.inStream);
    LOG.debug("GOT MessageType.SEQFILE_OPEN - Path: " + path);
    LOG.debug("GOT MessageType.SEQFILE_OPEN - Option: " + option);
    LOG.debug("GOT MessageType.SEQFILE_OPEN - KeyClass: " + keyClass);
    LOG.debug("GOT MessageType.SEQFILE_OPEN - ValueClass: " + valueClass);

    int fileID = -1;

    if (option.equals("r")) {
      SequenceFile.Reader reader;
      try {
        reader = new SequenceFile.Reader(fs, new Path(path), conf);

        if (reader.getKeyClassName().equals(keyClass)
            && reader.getValueClassName().equals(valueClass)) {
          // try to load key and value class
          Class<?> sequenceKeyClass = conf.getClassLoader().loadClass(keyClass);
          Class<?> sequenceValueClass = conf.getClassLoader().loadClass(
              valueClass);

          // try to instantiate key and value class
          Writable sequenceKeyWritable = (Writable) ReflectionUtils
              .newInstance(sequenceKeyClass, conf);
          Writable sequenceValueWritable = (Writable) ReflectionUtils
              .newInstance(sequenceValueClass, conf);

          // put new fileID and key and value Writable instances into HashMap
          fileID = reader.hashCode();
          this.sequenceFileReaders
              .put(
                  fileID,
                  new AbstractMap.SimpleEntry<SequenceFile.Reader, Entry<Writable, Writable>>(
                      reader, new AbstractMap.SimpleEntry<Writable, Writable>(
                          sequenceKeyWritable, sequenceValueWritable)));

        } else { // keyClass or valueClass is wrong
          fileID = -1;
          if (!reader.getKeyClassName().equals(keyClass)) {
            LOG.error("SEQFILE_OPEN - Wrong KeyClass: " + keyClass
                + " File KeyClass: " + reader.getKeyClassName());
          } else {
            LOG.error("SEQFILE_OPEN - Wrong ValueClass: " + valueClass
                + " File ValueClass: " + reader.getValueClassName());
          }
        }

      } catch (IOException e) {
        LOG.error("SEQFILE_OPEN - " + e.getMessage());
        fileID = -1;
      } catch (ClassNotFoundException e) {
        LOG.error("SEQFILE_OPEN - " + e.getMessage());
        fileID = -1;
      }

    } else if (option.equals("w")) {
      SequenceFile.Writer writer;
      try {
        // SequenceFile.Writer has an exclusive lease for a file
        // No other client can write to this file until other Writer has
        // completed
        if (!this.sequenceFileWriterPaths.contains(path)) {

          // try to load key and value class
          Class<?> sequenceKeyClass = conf.getClassLoader().loadClass(keyClass);
          Class<?> sequenceValueClass = conf.getClassLoader().loadClass(
              valueClass);

          // try to instantiate key and value class
          Writable sequenceKeyWritable = (Writable) ReflectionUtils
              .newInstance(sequenceKeyClass, conf);
          Writable sequenceValueWritable = (Writable) ReflectionUtils
              .newInstance(sequenceValueClass, conf);

          writer = new SequenceFile.Writer(fs, conf, new Path(path),
              sequenceKeyClass, sequenceValueClass);

          // put new fileID and key and value Writable instances into HashMap
          fileID = writer.hashCode();
          this.sequenceFileWriters
              .put(
                  fileID,
                  new AbstractMap.SimpleEntry<SequenceFile.Writer, Entry<Writable, Writable>>(
                      writer, new AbstractMap.SimpleEntry<Writable, Writable>(
                          sequenceKeyWritable, sequenceValueWritable)));

          // add path to set (exclusive access)
          this.sequenceFileWriterPaths.add(path);

        } else { // Path was already opened by another SequenceFile.Writer
          fileID = -1;
          LOG.error("SEQFILE_OPEN - Path: " + path
              + " is already used by another Writer!");
        }

      } catch (IOException e) {
        LOG.error("SEQFILE_OPEN - " + e.getMessage());
        fileID = -1;
      } catch (ClassNotFoundException e) {
        LOG.error("SEQFILE_OPEN - " + e.getMessage());
        fileID = -1;
      }
    } else { // wrong option
      LOG.error("SEQFILE_OPEN - Wrong option: '" + option + "'");
    }

    WritableUtils.writeVInt(this.outStream, MessageType.SEQFILE_OPEN.code);
    WritableUtils.writeVInt(this.outStream, fileID);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEQFILE_OPEN - FileID: " + fileID);
  }

  public void seqFileReadNext() throws IOException {
    int fileID = WritableUtils.readVInt(this.inStream);
    LOG.debug("GOT MessageType.SEQFILE_READNEXT - FileID: " + fileID);

    // check if fileID is available in sequenceFileReader
    if (this.sequenceFileReaders.containsKey(fileID)) {

      Writable sequenceKeyWritable = sequenceFileReaders.get(fileID).getValue()
          .getKey();
      Writable sequenceValueWritable = sequenceFileReaders.get(fileID)
          .getValue().getValue();

      // try to read next key/value pair from SequenceFile.Reader
      if (this.sequenceFileReaders.get(fileID).getKey()
          .next(sequenceKeyWritable, sequenceValueWritable)) {

        WritableUtils.writeVInt(this.outStream,
            MessageType.SEQFILE_READNEXT.code);
        binProtocol.writeObject(sequenceKeyWritable);
        binProtocol.writeObject(sequenceValueWritable);

        LOG.debug("Responded MessageType.SEQFILE_READNEXT -"
            + " Key: "
            + ((sequenceKeyWritable.toString().length() < 10) ? sequenceKeyWritable
                .toString() : sequenceKeyWritable.toString().substring(0, 9)
                + "...")
            + " Value: "
            + ((sequenceValueWritable.toString().length() < 10) ? sequenceValueWritable
                .toString() : sequenceValueWritable.toString().substring(0, 9)
                + "..."));

      } else { // false when at end of file
        WritableUtils.writeVInt(this.outStream, MessageType.END_OF_DATA.code);
        LOG.debug("Responded MessageType.SEQFILE_READNEXT - END_OF_DATA");
      }
      binProtocol.flush();

    } else { // no fileID stored
      LOG.error("MessageType.SEQFILE_READNEXT: FileID " + fileID
          + " not found!");
      WritableUtils.writeVInt(this.outStream, MessageType.END_OF_DATA.code);
      LOG.debug("Responded MessageType.SEQFILE_READNEXT - END_OF_DATA");
      binProtocol.flush();
    }
  }

  public void seqFileAppend() throws IOException {
    int fileID = WritableUtils.readVInt(this.inStream);
    LOG.debug("GOT MessageType.SEQFILE_APPEND - FileID: " + fileID);

    boolean result = false;

    // check if fileID is available in sequenceFileWriter
    if (this.sequenceFileWriters.containsKey(fileID)) {

      Writable sequenceKeyWritable = sequenceFileWriters.get(fileID).getValue()
          .getKey();
      Writable sequenceValueWritable = sequenceFileWriters.get(fileID)
          .getValue().getValue();

      // try to read key and value
      readObject(sequenceKeyWritable);
      readObject(sequenceValueWritable);

      if ((sequenceKeyWritable != null) && (sequenceValueWritable != null)) {

        // append to sequenceFile
        this.sequenceFileWriters.get(fileID).getKey()
            .append(sequenceKeyWritable, sequenceValueWritable);

        LOG.debug("Stored data: Key: "
            + ((sequenceKeyWritable.toString().length() < 10) ? sequenceKeyWritable
                .toString() : sequenceKeyWritable.toString().substring(0, 9)
                + "...")
            + " Value: "
            + ((sequenceValueWritable.toString().length() < 10) ? sequenceValueWritable
                .toString() : sequenceValueWritable.toString().substring(0, 9)
                + "..."));

        result = true;
      }
    } else { // no fileID stored

      // Skip written data from InputStream
      int availableBytes = this.inStream.available();
      this.inStream.skip(availableBytes);
      LOG.debug("MessageType.SEQFILE_APPEND: skip " + availableBytes + " bytes");
      LOG.error("MessageType.SEQFILE_APPEND: FileID " + fileID + " not found!");
    }

    // RESPOND
    WritableUtils.writeVInt(this.outStream, MessageType.SEQFILE_APPEND.code);
    WritableUtils.writeVInt(this.outStream, result ? 1 : 0);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEQFILE_APPEND - Result: " + result);
  }

  public void seqFileClose() throws IOException {
    int fileID = WritableUtils.readVInt(this.inStream);
    LOG.debug("GOT MessageType.SEQFILE_CLOSE - FileID: " + fileID);

    boolean result = false;

    if (this.sequenceFileReaders.containsKey(fileID)) {
      this.sequenceFileReaders.get(fileID).getKey().close();
      this.sequenceFileReaders.remove(fileID);
      result = true;
    } else if (this.sequenceFileWriters.containsKey(fileID)) {
      this.sequenceFileWriters.get(fileID).getKey().close();
      this.sequenceFileWriters.remove(fileID);
      result = true;
    } else { // no fileID stored
      LOG.error("MessageType.SEQFILE_CLOSE: FileID " + fileID + " not found!");
    }

    // RESPOND
    WritableUtils.writeVInt(this.outStream, MessageType.SEQFILE_CLOSE.code);
    WritableUtils.writeVInt(this.outStream, result ? 1 : 0);
    binProtocol.flush();
    LOG.debug("Responded MessageType.SEQFILE_CLOSE - Result: " + result);
  }

  public void partitionResponse() throws IOException {
    int partResponse = WritableUtils.readVInt(this.inStream);
    synchronized (binProtocol.resultLock) {
      binProtocol.setResult(partResponse);
      LOG.debug("Received MessageType.PARTITION_RESPONSE - Result: "
          + partResponse);
      binProtocol.resultLock.notify();
    }
  }

  /**
   * Read the given object from stream. If it is a IntWritable, LongWritable,
   * FloatWritable, DoubleWritable, Text or BytesWritable, read it directly.
   * Otherwise, read it to a buffer and then write the length and data to the
   * stream.
   * 
   * @param obj the object to read
   * @throws IOException
   */
  protected void readObject(Writable obj) throws IOException {
    byte[] buffer;
    // For BytesWritable and Text, use the specified length to set the length
    // this causes the "obvious" translations to work. So that if you emit
    // a string "abc" from C++, it shows up as "abc".
    if (obj instanceof Text) {
      int numBytes = WritableUtils.readVInt(this.inStream);
      buffer = new byte[numBytes];
      this.inStream.readFully(buffer);
      ((Text) obj).set(buffer);

    } else if (obj instanceof BytesWritable) {
      int numBytes = WritableUtils.readVInt(this.inStream);
      buffer = new byte[numBytes];
      this.inStream.readFully(buffer);
      ((BytesWritable) obj).set(buffer, 0, numBytes);

    } else if (obj instanceof IntWritable) {
      ((IntWritable) obj).set(WritableUtils.readVInt(this.inStream));

    } else if (obj instanceof LongWritable) {
      ((LongWritable) obj).set(WritableUtils.readVLong(this.inStream));

    } else {
      try {
        LOG.debug("reading type: " + obj.getClass().getName());

        // try reading object
        obj.readFields(this.inStream);

      } catch (IOException e) {
        throw new IOException("Hama Pipes is not able to read "
            + obj.getClass().getName(), e);
      }
    }
  }
}
