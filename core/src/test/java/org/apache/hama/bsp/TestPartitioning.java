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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.KeyValuePair;
import org.apache.zookeeper.KeeperException;

public class TestPartitioning extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestPartitioning.class);

  public void testPartitioner() throws Exception {

    Configuration conf = new Configuration();
    conf.set("bsp.local.dir", "/tmp/hama-test/partitioning");
    conf.set("bsp.partitioning.dir", "/tmp/hama-test/partitioning/localtest");
    BSPJob bsp = new BSPJob(new HamaConfiguration(conf));
    bsp.setJobName("Test partitioning with input");
    bsp.setBspClass(PartionedBSP.class);
    bsp.setNumBspTask(2);
    conf.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 600);
    bsp.setInputFormat(TextInputFormat.class);
    bsp.setOutputFormat(NullOutputFormat.class);
    bsp.setInputPath(new Path("../CHANGES.txt"));
    bsp.setPartitioner(HashPartitioner.class);
    assertTrue(bsp.waitForCompletion(true));
  }

  public static class PartionedBSP extends
      BSP<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    public void bsp(BSPPeer<LongWritable, Text, NullWritable, NullWritable> peer)
        throws IOException, KeeperException, InterruptedException {
      long numOfPairs = 0;
      KeyValuePair<LongWritable, Text> readNext = null;
      while ((readNext = peer.readNext()) != null) {
        LOG.debug(readNext.getKey().get() + " / " + readNext.getValue().toString());
        numOfPairs++;
      }

      assertTrue(numOfPairs > 2);
    }

  }

}
