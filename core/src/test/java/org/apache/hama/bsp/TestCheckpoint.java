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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.messages.ByteMessage;

public class TestCheckpoint extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestCheckpoint.class);

  static final String checkpointedDir = "checkpoint/job_201110302255_0001/0/";

  @SuppressWarnings("unchecked")
  public void testCheckpoint() throws Exception {
    Configuration config = new HamaConfiguration();
    FileSystem dfs = FileSystem.get(config);

    BSPPeerImpl bspTask = new BSPPeerImpl(config, dfs);
    assertNotNull("BSPPeerImpl should not be null.", bspTask);
    if(dfs.mkdirs(new Path("checkpoint"))) {
      if(dfs.mkdirs(new Path("checkpoint/job_201110302255_0001"))) {
        if(dfs.mkdirs(new Path("checkpoint/job_201110302255_0001/0")));
      }
    }
    assertTrue("Make sure directory is created.", 
               dfs.exists(new Path(checkpointedDir)));
    byte[] tmpData = "data".getBytes();
    BSPMessageBundle bundle = new BSPMessageBundle();
    bundle.addMessage(new ByteMessage("abc".getBytes(), tmpData));
    assertNotNull("Message bundle can not be null.", bundle);
    assertNotNull("Configuration should not be null.", config);
    bspTask.checkpoint(checkpointedDir+"/attempt_201110302255_0001_000000_0", 
                       bundle);
    FSDataInputStream in = dfs.open(new Path(checkpointedDir+
      "/attempt_201110302255_0001_000000_0"));
    BSPMessageBundle bundleRead = new BSPMessageBundle();
    bundleRead.readFields(in);
    in.close();
    ByteMessage byteMsg = (ByteMessage)(bundleRead.getMessages()).get(0);
    String content = new String(byteMsg.getData());
    LOG.info("Saved checkpointed content is "+content);
    assertTrue("Message content should be the same.",  "data".equals(content)); 
    dfs.delete(new Path("checkpoint"), true);
  }
}
