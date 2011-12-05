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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.sync.SyncException;

public class IOSerializePrinting extends
    BSP<NullWritable, NullWritable, LongWritable, Text> {

  public static final Log LOG = LogFactory.getLog(IOSerializePrinting.class);
  private final static int PRINT_INTERVAL = 1000;
  private int num;

  public void bsp(BSPPeer<NullWritable, NullWritable, LongWritable, Text> peer)
      throws IOException, SyncException, InterruptedException {

    int i = 0;
    for (String otherPeer : peer.getAllPeerNames()) {
      String peerName = peer.getPeerName();
      if (peerName.equals(otherPeer)) {
        peer.write(new LongWritable(System.currentTimeMillis()), new Text(
            "Hello BSP from " + (i + 1) + " of " + num + ": " + peerName));
      }

      Thread.sleep(PRINT_INTERVAL);
      peer.sync();
      i++;
    }
  }

  public void setup(
      org.apache.hama.bsp.BSPPeer<NullWritable, NullWritable, LongWritable, Text> peer)
      throws IOException, SyncException, InterruptedException {
    num = Integer.parseInt(conf.get("bsp.peers.num"));

  }
}
