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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;

public class TestBSPMasterGroomServer extends HamaCluster {

  private static Log LOG = LogFactory.getLog(TestBSPMasterGroomServer.class);
  private static String TMP_OUTPUT = "/tmp/test-example/";
  private HamaConfiguration configuration;
  private String TEST_JOB = "src/test/java/testjar/testjob.jar";

  public TestBSPMasterGroomServer() {
    configuration = new HamaConfiguration();
    configuration.set("bsp.master.address", "localhost");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.setStrings("bsp.local.dir", "/tmp/hama-test");
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT, 21810);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testSubmitJob() throws Exception {
    BSPJob bsp = new BSPJob(configuration);
    bsp.setJobName("Test Serialize Printing");
    bsp.setBspClass(testjar.ClassSerializePrinting.HelloBSP.class);
    bsp.setJar(System.getProperty("user.dir")+"/"+TEST_JOB);

    // Set the task size as a number of GroomServer
    BSPJobClient jobClient = new BSPJobClient(configuration);
    configuration.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 600);
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    assertEquals(this.numOfGroom, cluster.getMaxTasks());
    
    // TODO test the multi-tasks 
    bsp.setNumBspTask(1);
    
    FileSystem fileSys = FileSystem.get(conf);

    if (bsp.waitForCompletion(true)) {
      checkOutput(fileSys, cluster, conf);
    }
    LOG.info("Client finishes execution job.");
  }

  private static void checkOutput(FileSystem fileSys, ClusterStatus cluster,
      HamaConfiguration conf) throws Exception {
    for (int i = 0; i < 1; i++) { // TODO test the multi-tasks
      SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, new Path(
          TMP_OUTPUT + i), conf);
      LongWritable timestamp = new LongWritable();
      Text message = new Text();
      reader.next(timestamp, message);
      assertTrue("Check if `Hello BSP' gets printed.", message.toString()
          .indexOf("Hello BSP from") >= 0);
      reader.close();
    }
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }
}
