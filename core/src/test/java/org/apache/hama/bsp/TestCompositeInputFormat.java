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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.join.CompositeInputFormat;
import org.apache.hama.bsp.join.TupleWritable;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;

public class TestCompositeInputFormat extends HamaCluster {
  protected HamaConfiguration configuration;

  public TestCompositeInputFormat() {
    configuration = new HamaConfiguration();
    configuration.set("bsp.master.address", "localhost");
    configuration.set("hama.child.redirect.log.console", "true");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.set("bsp.local.dir", "/tmp/hama-test");
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT, 21810);
    configuration.set("hama.sync.client.class",
        org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl.class
            .getCanonicalName());
  }

  public static final Log LOG = LogFactory
      .getLog(TestCompositeInputFormat.class);

  public void testCompositeInputFormat() throws IOException,
      ClassNotFoundException, InterruptedException {
    generateTestData();

    HamaConfiguration conf = new HamaConfiguration();
    BSPJob job = new BSPJob(conf);

    FileInputFormat.setInputPaths(job, "/tmp/a.dat,/tmp/b.dat");

    job.setInputFormat(CompositeInputFormat.class);
    job.set("bsp.join.expr", CompositeInputFormat.compose("outer",
        SequenceFileInputFormat.class, FileInputFormat.getInputPaths(job)));
    job.setOutputFormat(NullOutputFormat.class);

    job.setBspClass(JoinBSP.class);
    job.waitForCompletion(true);

    FileSystem fs = FileSystem.get(conf);

    fs.delete(new Path("/tmp/a.dat"), true);
    fs.delete(new Path("/tmp/b.dat"), true);
  }

  public static class JoinBSP extends
      BSP<IntWritable, TupleWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void bsp(
        BSPPeer<IntWritable, TupleWritable, NullWritable, NullWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<IntWritable, TupleWritable> readNext = null;
      while ((readNext = peer.readNext()) != null) {
        int a = ((IntWritable) readNext.getValue().get(0)).get();
        int b = ((IntWritable) readNext.getValue().get(1)).get();

        assertEquals(a + b, 10);
      }
    }
  }

  private void generateTestData() {
    try {
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, configuration,
          new Path("/tmp/a.dat"), IntWritable.class, IntWritable.class);
      for (int i = 0; i < 10; i++) {
        writer.append(new IntWritable(i), new IntWritable(i));
      }
      writer.close();

      writer = SequenceFile.createWriter(fs, configuration, new Path(
          "/tmp/b.dat"), IntWritable.class, IntWritable.class);
      for (int i = 0; i < 10; i++) {
        writer.append(new IntWritable(i), new IntWritable(10 - i));
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
