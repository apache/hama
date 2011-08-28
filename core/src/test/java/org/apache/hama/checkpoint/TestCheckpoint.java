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
package org.apache.hama.checkpoint;

import java.io.DataInput;
import java.io.DataInputStream;
import java.util.List;

import junit.framework.TestCase;
import static junit.framework.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPMessage;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.BSPPeer.BSPSerializableMessage;
import org.apache.hama.bsp.BSPSerializerWrapper;
import org.apache.hama.bsp.DoubleMessage;
import org.apache.hama.HamaConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestCheckpoint extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestCheckpoint.class);

  private CheckpointRunner runner;
  private BSPSerializerWrapper serializer;
  static final String TEST_STRING = "Test String";
  private FileSystem hdfs;
  static final DoubleMessage estimate = 
    new DoubleMessage("192.168.1.123:61000", 3.1415926d);

  public void setUp() throws Exception {
    Configuration conf = new HamaConfiguration();
    this.hdfs = FileSystem.get(conf);
    assertNotNull("File system object should exist.", this.hdfs);
    this.runner =  
      new CheckpointRunner(CheckpointRunner.buildCommands(conf));
    assertNotNull("Checkpoint instance should exist.", this.runner);
    this.runner.start();
    Thread.sleep(1000*1);
    Process process = this.runner.getProcess();
    assertNotNull("Checkpoint process should be created.", process);
    this.serializer = new BSPSerializerWrapper(conf, 
      Integer.parseInt(CheckpointRunner.DEFAULT_PORT));
  }

  private BSPMessageBundle createMessageBundle() {
    BSPMessageBundle bundle = new BSPMessageBundle();
    bundle.addMessage(estimate);
    return bundle;
  }

  private String checkpointedPath() {
      return "/tmp/" + "job_201108221205_000" + "/" + "0" +
      "/" + "attempt_201108221205_0001_000000_0";
  }

  public void testCheckpoint() throws Exception {
    this.serializer.serialize(new BSPSerializableMessage(
    checkpointedPath(), createMessageBundle()));
    Thread.sleep(1000); 
    Path path = new Path(checkpointedPath());
    boolean exists = this.hdfs.exists(path);
    assertTrue("Check if file is actually written to hdfs.", exists); 
    BSPMessageBundle bundle = new BSPMessageBundle(); 
    DataInput in = new DataInputStream(this.hdfs.open(path));
    bundle.readFields(in);
    List<BSPMessage> messages = bundle.getMessages();
    assertEquals("Only one message exists.", 1,  messages.size());
    for(BSPMessage message: messages) {
      String peer = (String)message.getTag();
      assertEquals("BSPPeer value in form of <ip>:<port>.", peer, estimate.getTag());
      Double pi = (Double)message.getData();
      assertEquals("Message content.", pi, estimate.getData());
    }
  }

  public void tearDown() throws Exception {
    this.runner.stop();
  }
  
}
