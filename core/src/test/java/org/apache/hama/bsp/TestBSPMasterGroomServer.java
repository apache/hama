/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.message.compress.SnappyCompressor;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.apache.hama.examples.ClassSerializePrinting;

public class TestBSPMasterGroomServer extends HamaCluster {

  protected static Log LOG = LogFactory.getLog(TestBSPMasterGroomServer.class);
  public static String TMP_OUTPUT = "/tmp/test-example/";
  public static final String TMP_OUTPUT_PATH = "/tmp/messageQueue";
  public static Path OUTPUT_PATH = new Path(TMP_OUTPUT + "serialout");

  protected HamaConfiguration configuration;

  // these variables are preventing from rebooting the whole stuff again since
  // setup and teardown are called per method.

  public TestBSPMasterGroomServer() {
    configuration = new HamaConfiguration();
    configuration.set("bsp.master.address", "localhost");
    configuration.set("hama.child.redirect.log.console", "true");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.set("bsp.local.dir", "/tmp/hama-test");
    configuration.set(DiskQueue.DISK_QUEUE_PATH_KEY, TMP_OUTPUT_PATH);
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT, 21810);
    configuration.set("hama.sync.client.class",
        org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl.class
            .getCanonicalName());
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /*
   * BEGIN: Job submission tests.
   */

  public void testSubmitJob() throws Exception {
    BSPJob bsp = new BSPJob(configuration,
        org.apache.hama.examples.ClassSerializePrinting.class);
    bsp.setJobName("Test Serialize Printing");
    bsp.setBspClass(org.apache.hama.examples.ClassSerializePrinting.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(Text.class);
    bsp.setOutputPath(OUTPUT_PATH);
    
    bsp.setCompressionCodec(SnappyCompressor.class);
    bsp.setCompressionThreshold(40);

    BSPJobClient jobClient = new BSPJobClient(configuration);
    configuration.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 6000);
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    assertEquals(this.numOfGroom, cluster.getGroomServers());
    bsp.setNumBspTask(2);

    FileSystem fileSys = FileSystem.get(configuration);

    if (bsp.waitForCompletion(true)) {
      checkOutput(fileSys, configuration, 2);
    } else {
      fail();
    }
    LOG.info("Client finishes execution job.");
  }

  public static void checkOutput(FileSystem fileSys, Configuration conf,
      int tasks) throws Exception {
    FileStatus[] listStatus = fileSys.listStatus(OUTPUT_PATH);
    assertEquals(listStatus.length, tasks);
    for (FileStatus status : listStatus) {
      if (!status.isDir()) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSys,
            status.getPath(), conf);
        int superStep = 0;
        int taskstep = 0;
        IntWritable key = new IntWritable();
        Text value = new Text();
        /*
         * The serialize printing task should write in each superstep
         * "tasks"-times its superstep, along with the hostname.
         */
        while (reader.next(key, value)) {
          assertEquals(superStep, key.get());
          taskstep++;
          if (taskstep % tasks == 0) {
            superStep++;
          }
        }
        reader.close();
        // the maximum should be the number of supersteps defined in the task
        assertEquals(superStep, ClassSerializePrinting.NUM_SUPERSTEPS);
      }
    }

    fileSys.delete(new Path(TMP_OUTPUT), true);
  }

  /*
   * END: Job submission tests.
   */

}
