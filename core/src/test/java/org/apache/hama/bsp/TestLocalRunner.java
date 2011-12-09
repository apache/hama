/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;

public class TestLocalRunner extends TestCase {

  public void testOutputJob() throws Exception {
    Configuration conf = new Configuration();
    conf.set("bsp.local.dir", "/tmp/hama-test");
    BSPJob bsp = new BSPJob(new HamaConfiguration(conf));
    bsp.setJobName("Test Serialize Printing with Output");
    
    bsp.setBspClass(org.apache.hama.examples.ClassSerializePrinting.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(Text.class);
    bsp.setOutputPath(TestBSPMasterGroomServer.OUTPUT_PATH);
    
    conf.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 600);
    bsp.setNumBspTask(2);
    bsp.setInputFormat(NullInputFormat.class);

    FileSystem fileSys = FileSystem.get(conf);

    if (bsp.waitForCompletion(true)) {
      TestBSPMasterGroomServer.checkOutput(fileSys, conf, 2);
    }
  }

}
