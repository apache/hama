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
package org.apache.hama.examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.IntegerMessage;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.NullOutputFormat;
import org.apache.zookeeper.KeeperException;

public class CombineExample {

  public static class MyBSP extends
      BSP<NullWritable, NullWritable, NullWritable, NullWritable> {
    public static final Log LOG = LogFactory.getLog(MyBSP.class);

    @Override
    public void bsp(BSPPeer<NullWritable, NullWritable, NullWritable, NullWritable> peer) throws IOException,
        KeeperException, InterruptedException {
      for (String peerName : peer.getAllPeerNames()) {
        peer.send(peerName, new IntegerMessage(peer.getPeerName(), 1));
        peer.send(peerName, new IntegerMessage(peer.getPeerName(), 2));
        peer.send(peerName, new IntegerMessage(peer.getPeerName(), 3));
      }
      peer.sync();

      IntegerMessage received;
      while ((received = (IntegerMessage) peer.getCurrentMessage()) != null) {
        LOG.info(received.getTag() + ": " + received.getData());
      }
    }

  }

  public static class SumCombiner extends Combiner {

    @Override
    public BSPMessageBundle combine(Iterable<BSPMessage> messages) {
      BSPMessageBundle bundle = new BSPMessageBundle();
      int sum = 0;

      Iterator<BSPMessage> it = messages.iterator();
      while (it.hasNext()) {
        sum += ((IntegerMessage) it.next()).getData();
      }

      bundle.addMessage(new IntegerMessage("Sum", sum));
      return bundle;
    }

  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    BSPJob bsp = new BSPJob(conf, CombineExample.class);
    // Set the job name
    bsp.setJobName("Combine Example");
    bsp.setBspClass(MyBSP.class);
    bsp.setCombinerClass(SumCombiner.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputFormat(NullOutputFormat.class);
    bsp.setNumBspTask(2);

    bsp.waitForCompletion(true);
  }
}
