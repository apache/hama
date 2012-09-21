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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

/**
 * Streaming protocol that inherits from the binary protocol. Basically it
 * writes everything as text to the peer, each command is separated by newlines.
 * To distinguish op-codes (like SET_BSPJOB_CONF) from normal output, we use the
 * surrounds %OP_CODE%=_possible_value.
 * 
 * @param <K1> input key.
 * @param <V1> input value.
 * @param <K2> output key.
 * @param <V2> output value.
 */
public class StreamingProtocol<K1 extends Writable, V1 extends Writable>
    extends BinaryProtocol<K1, V1, Text, Text> {

  private static final Pattern PROTOCOL_STRING_PATTERN = Pattern.compile("=");

  private final CyclicBarrier ackBarrier = new CyclicBarrier(2);
  private volatile boolean brokenBarrier = false;

  public StreamingProtocol(BSPPeer<K1, V1, Text, Text, BytesWritable> peer,
      OutputStream out, InputStream in) throws IOException {
    super(peer, out, in);
  }

  public class StreamingUplinkReaderThread extends UplinkReaderThread {

    private BufferedReader reader;

    public StreamingUplinkReaderThread(
        BSPPeer<K1, V1, Text, Text, BytesWritable> peer, InputStream stream)
        throws IOException {
      super(peer, stream);
      reader = new BufferedReader(new InputStreamReader(inStream));
    }

    @Override
    public void sendMessage() throws IOException {
      String peerLine = reader.readLine();
      String msgLine = reader.readLine();
      peer.send(peerLine, new BytesWritable(msgLine.getBytes()));
    }

    @Override
    public void getMessage() throws IOException {
      BytesWritable currentMessage = peer.getCurrentMessage();
      if (currentMessage != null)
        writeLine(new String(currentMessage.getBytes()));
      else
        writeLine("%%-1%%");
    }

    @Override
    public void getMessageCount() throws IOException {
      writeLine("" + peer.getNumCurrentMessages());
    }

    @Override
    public void getSuperstepCount() throws IOException {
      writeLine("" + peer.getSuperstepCount());
    }

    @Override
    public void getPeerName() throws IOException {
      int id = Integer.parseInt(reader.readLine());
      if (id == -1)
        writeLine(peer.getPeerName());
      else
        writeLine(peer.getPeerName(id));
    }

    @Override
    public void getPeerIndex() throws IOException {
      writeLine("" + peer.getPeerIndex());
    }

    @Override
    public void getAllPeerNames() throws IOException {
      writeLine("" + peer.getAllPeerNames().length);
      for (String s : peer.getAllPeerNames()) {
        writeLine(s);
      }
    }

    @Override
    public void getPeerCount() throws IOException {
      writeLine("" + peer.getAllPeerNames().length);
    }

    @Override
    public void sync() throws IOException, SyncException, InterruptedException {
      peer.sync();
      writeLine(getProtocolString(MessageType.SYNC) + "_SUCCESS");
    }

    @Override
    public void writeKeyValue() throws IOException {
      String key = reader.readLine();
      String value = reader.readLine();
      peer.write(new Text(key), new Text(value));
    }

    @Override
    public void readKeyValue() throws IOException {
      KeyValuePair<K1, V1> readNext = peer.readNext();
      if (readNext == null) {
        writeLine("%%-1%%");
        writeLine("%%-1%%");
      } else {
        writeLine(readNext.getKey() + "");
        writeLine(readNext.getValue() + "");
      }
    }

    @Override
    public void reopenInput() throws IOException {
      peer.reopenInput();
    }

    @Override
    public int readCommand() throws IOException {
      String readLine = reader.readLine();
      if (readLine != null && !readLine.isEmpty()) {
        String[] split = PROTOCOL_STRING_PATTERN.split(readLine, 2);
        split[0] = split[0].replace("%", "");
        if (checkAcks(split))
          return -1;
        try {
          int parseInt = Integer.parseInt(split[0]);
          if (parseInt == BinaryProtocol.MessageType.LOG.code) {
            LOG.info(split[1]);
            return -1;
          }
          return parseInt;
        } catch (NumberFormatException e) {
          e.printStackTrace();
        }
      } else {
        return -1;
      }
      return -2;
    }

    @Override
    protected void onError(Throwable e) {
      super.onError(e);
      // break the barrier if we had an error
      ackBarrier.reset();
      brokenBarrier = true;
    }

    private boolean checkAcks(String[] readLine) {
      if (readLine[0].startsWith("ACK_")) {
        try {
          ackBarrier.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (BrokenBarrierException e) {
          e.printStackTrace();
        }
        return true;
      }
      return false;
    }

  }

  @Override
  public void start() throws IOException {
    writeLine(MessageType.START, null);
    writeLine("" + CURRENT_PROTOCOL_VERSION);
    setBSPJob(peer.getConfiguration());
    try {
      ackBarrier.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (BrokenBarrierException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setBSPJob(Configuration conf) throws IOException {
    writeLine(MessageType.SET_BSPJOB_CONF, null);
    List<String> list = new ArrayList<String>();
    for (Map.Entry<String, String> itm : conf) {
      list.add(itm.getKey());
      list.add(itm.getValue());
    }
    writeLine(list.size());
    for (String entry : list) {
      writeLine(entry);
    }
    flush();
  }

  @Override
  public void runSetup(boolean pipedInput, boolean pipedOutput)
      throws IOException {
    writeLine(MessageType.RUN_SETUP, null);
    waitOnAck();
  }

  @Override
  public void runBsp(boolean pipedInput, boolean pipedOutput)
      throws IOException {
    writeLine(MessageType.RUN_BSP, null);
    waitOnAck();
  }

  public void waitOnAck() {
    try {
      if (!brokenBarrier)
        ackBarrier.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (BrokenBarrierException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void runCleanup(boolean pipedInput, boolean pipedOutput)
      throws IOException {
    writeLine(MessageType.RUN_CLEANUP, null);
    waitOnAck();
  }

  @Override
  public UplinkReaderThread getUplinkReader(
      BSPPeer<K1, V1, Text, Text, BytesWritable> peer, InputStream in)
      throws IOException {
    return new StreamingUplinkReaderThread(peer, in);
  }

  public void writeLine(int msg) throws IOException {
    writeLine("" + msg);
  }

  public void writeLine(String msg) throws IOException {
    stream.write((msg + "\n").getBytes());
    stream.flush();
  }

  public void writeLine(MessageType type, String msg) throws IOException {
    stream.write((getProtocolString(type) + (msg == null ? "" : msg) + "\n")
        .getBytes());
    stream.flush();
  }

  public String getProtocolString(MessageType type) {
    return "%" + type.code + "%=";
  }

}
