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
package testjar;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeerProtocol;
import org.apache.zookeeper.KeeperException;

public class ClassSerializePrinting {
  private static String TMP_OUTPUT = "/tmp/test-example/";

  public static class HelloBSP extends BSP {
    public static final Log LOG = LogFactory.getLog(HelloBSP.class);
    private Configuration conf;
    private final static int PRINT_INTERVAL = 1000;
    private FileSystem fileSys;
    private int num;

    public void bsp(BSPPeerProtocol bspPeer) throws IOException,
        KeeperException, InterruptedException {

      int i = 0;
      for (String otherPeer : bspPeer.getAllPeerNames()) {
        String peerName = bspPeer.getPeerName();
        if (peerName.equals(otherPeer)) {
          writeLogToFile(peerName, i);
        }

        Thread.sleep(PRINT_INTERVAL);
        bspPeer.sync();
        i++;
      }
    }

    private void writeLogToFile(String string, int i) throws IOException {
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
          new Path(TMP_OUTPUT + i), LongWritable.class, Text.class,
          CompressionType.NONE);
      writer.append(new LongWritable(System.currentTimeMillis()), new Text(
          "Hello BSP from " + (i + 1) + " of " + num + ": " + string));
      writer.close();
    }

    public Configuration getConf() {
      return conf;
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
      num = Integer.parseInt(conf.get("bsp.peers.num"));
      try {
        fileSys = FileSystem.get(conf);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

}
