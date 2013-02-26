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
package org.apache.hama.examples.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.sync.SyncException;

import com.google.common.collect.Sets;

public class FastGraphGen {
  protected static Log LOG = LogFactory.getLog(FastGraphGen.class);

  private static String SIZE_OF_MATRIX = "size.of.matrix";
  private static String MAX_EDGES = "max.outlinks";

  public static class FastGraphGenBSP extends
      BSP<NullWritable, NullWritable, Text, TextArrayWritable, Text> {

    private Configuration conf;
    private int sizeN;
    private int maxOutEdges;

    @Override
    public void setup(
        BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer) {
      this.conf = peer.getConfiguration();
      sizeN = conf.getInt(SIZE_OF_MATRIX, 10);
      maxOutEdges = conf.getInt(MAX_EDGES, 1);
    }

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, Text, TextArrayWritable, Text> peer)
        throws IOException, SyncException, InterruptedException {
      int interval = sizeN / peer.getNumPeers();
      int startID = peer.getPeerIndex() * interval;
      int endID;
      if (peer.getPeerIndex() == peer.getNumPeers() - 1) {
        endID = sizeN;
      } else {
        endID = startID + interval;
      }

      Random r = new Random();
      for (int i = startID; i < endID; i++) {
        HashSet<Integer> set = Sets.newHashSet();
        for (int j = 0; j < maxOutEdges; j++) {
          set.add(r.nextInt(sizeN));
        }
        TextArrayWritable textArrayWritable = new TextArrayWritable();
        Text[] arr = new Text[set.size()];
        int index = 0;
        for (int x : set) {
          arr[index++] = new Text(x + "");
        }
        textArrayWritable.set(arr);
        peer.write(new Text(i + ""), textArrayWritable);
      }

    }
  }

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    if (args.length < 4) {
      System.out
          .println("Usage: <size n> <max out-edges> <output path> <number of tasks>");
      System.exit(1);
    }

    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();

    conf.setInt(SIZE_OF_MATRIX, Integer.parseInt(args[0]));
    conf.setInt(MAX_EDGES, Integer.parseInt(args[1]));

    BSPJob bsp = new BSPJob(conf, FastGraphGenBSP.class);
    // Set the job name
    bsp.setJobName("Random Fast Matrix Generator");
    bsp.setBspClass(FastGraphGenBSP.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(TextArrayWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, new Path(args[2]));
    bsp.setNumBspTask(Integer.parseInt(args[3]));

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
