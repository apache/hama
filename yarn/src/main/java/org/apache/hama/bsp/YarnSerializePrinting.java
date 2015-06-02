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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.SyncException;

public class YarnSerializePrinting {

  public static Path OUTPUT_PATH = new Path("/tmp/serialout");

  public static class HelloBSP extends
      BSP<NullWritable, NullWritable, IntWritable, Text, NullWritable> {
    public static final Log LOG = LogFactory.getLog(HelloBSP.class);
    private int num;

    @Override
    public void bsp(
        BSPPeer<NullWritable, NullWritable, IntWritable, Text, NullWritable> bspPeer)
        throws IOException, SyncException, InterruptedException {
      num = bspPeer.getConfiguration().getInt("bsp.peers.num", 1);
      IntWritable peerNum = new IntWritable();
      Text txt = new Text();
      int i = 0;
      for (String otherPeer : bspPeer.getAllPeerNames()) {
        String peerName = bspPeer.getPeerName();
        if (peerName.equals(otherPeer)) {
          peerNum.set(i);
          txt.set("Hello BSP from " + (i + 1) + " of " + num + ": " + peerName);
          bspPeer.write(null, txt);
        }

        bspPeer.sync();
        i++;
      }
    }
  }

  static void printOutput(HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(OUTPUT_PATH);
    for (FileStatus file : files) {
      if (file.getLen() > 0) {
        FSDataInputStream in = fs.open(file.getPath());
        IOUtils.copyBytes(in, System.out, conf, false);
        in.close();
      }
    }

    fs.delete(OUTPUT_PATH, true);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();
    conf.set("hama.home.dir", System.getenv().get("HAMA_HOME"));

    YARNBSPJob job = new YARNBSPJob(conf);
    job.setBspClass(HelloBSP.class);
    job.setJarByClass(HelloBSP.class);
    job.setJobName("Serialize Printing");
    job.setInputFormat(NullInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputPath(OUTPUT_PATH);
    job.setMemoryUsedPerTaskInMb(100);
    job.setNumBspTask(4);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    printOutput(conf);
    System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }
}
