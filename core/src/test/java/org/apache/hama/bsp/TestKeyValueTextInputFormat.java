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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.junit.Test;

public class TestKeyValueTextInputFormat extends TestCase {

  public static class KeyValueHashPartitionedBSP extends
      BSP<Text, Text, NullWritable, NullWritable, MapWritable> {
    public static final String TEST_INPUT_VALUES = "test.bsp.max.input";
    public static final String TEST_UNEXPECTED_KEYS = "test.bsp.keys.unexpected";
    public static final String TEST_MAX_VALUE = "test.bsp.keys.max";

    private int numTasks = 0;
    private int maxValue = 0;

    @Override
    public void setup(
        BSPPeer<Text, Text, NullWritable, NullWritable, MapWritable> peer)
        throws IOException, SyncException, InterruptedException {
      Configuration conf = peer.getConfiguration();
      maxValue = conf.getInt(KeyValueHashPartitionedBSP.TEST_MAX_VALUE, 1000);
      numTasks = peer.getNumPeers();
    }

    @Override
    public void bsp(
        BSPPeer<Text, Text, NullWritable, NullWritable, MapWritable> peer)
        throws IOException, SyncException, InterruptedException {
      MapWritable expectedKeys = new MapWritable();
      
      Text key = null;
      Text value = null;
      MapWritable message = new MapWritable();
      message.put(new Text(KeyValueHashPartitionedBSP.TEST_UNEXPECTED_KEYS),
          new BooleanWritable(false));
      KeyValuePair<Text, Text> tmp = null;

      while ((tmp = peer.readNext()) != null) {
        key = tmp.getKey();
        value = tmp.getValue();

        int expectedPeerId = Math.abs(key.hashCode() % numTasks);

        if (expectedPeerId == peer.getPeerIndex()) {
          expectedKeys.put(new Text(key), new Text(value));
        } else {
          message.put(
              new Text(KeyValueHashPartitionedBSP.TEST_UNEXPECTED_KEYS),
              new BooleanWritable(true));
          break;
        }
      }
      message.put(new Text(KeyValueHashPartitionedBSP.TEST_INPUT_VALUES),
          expectedKeys);

      int master = peer.getNumPeers() / 2;
      peer.send(peer.getPeerName(master), message);
      peer.sync();

      if (peer.getPeerIndex() == master) {
        MapWritable msg = null;
        MapWritable values = null;
        BooleanWritable blValue = null;
        HashMap<Integer, Integer> input = new HashMap<Integer, Integer>();

        while ((msg = peer.getCurrentMessage()) != null) {
          blValue = (BooleanWritable) msg.get(new Text(
              KeyValueHashPartitionedBSP.TEST_UNEXPECTED_KEYS));
          assertEquals(false, blValue.get());
          values = (MapWritable) msg.get(new Text(
              KeyValueHashPartitionedBSP.TEST_INPUT_VALUES));
          for (Map.Entry<Writable, Writable> w : values.entrySet()) {
            input.put(Integer.valueOf(w.getKey().toString()),
                Integer.valueOf(w.getValue().toString()));
          }
        }

        for (int i = 0; i < maxValue; i++) {
          assertEquals(true, input.containsKey(Integer.valueOf(i)));
          assertEquals(i * i, input.get(Integer.valueOf(i)).intValue());
        }
      }
      peer.sync();
    }
  }

  @Test
  public void testInput() throws IOException {

    Configuration fsConf = new Configuration();
    String strDataPath = "/tmp/test_keyvalueinputformat";
    Path dataPath = new Path(strDataPath);
    Path outPath = new Path("/tmp/test_keyvalueinputformat_out");

    int maxValue = 1000;
    FileSystem fs = null;

    try {
      URI uri = new URI(strDataPath);
      fs = FileSystem.get(uri, fsConf);
      fs.delete(dataPath, true);
      if (fs.exists(outPath)) {
        fs.delete(outPath, true);
      }
      FSDataOutputStream fileOut = fs.create(dataPath, true);

      StringBuilder str = new StringBuilder();
      for (int i = 0; i < maxValue; ++i) {
        str.append(i);
        str.append("\t");
        str.append(i * i);
        str.append("\n");
      }
      fileOut.writeBytes(str.toString());
      fileOut.close();

    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      HamaConfiguration conf = new HamaConfiguration();
      conf.setInt(KeyValueHashPartitionedBSP.TEST_MAX_VALUE, maxValue);
      BSPJob job = new BSPJob(conf, TestKeyValueTextInputFormat.class);
      job.setJobName("Test KeyValueTextInputFormat together with HashPartitioner");
      job.setBspClass(KeyValueHashPartitionedBSP.class);

      job.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, true);
      job.set(Constants.RUNTIME_PARTITIONING_DIR, "/tmp/hamatest/parts");
      job.setPartitioner(HashPartitioner.class);

      job.setNumBspTask(2);
      job.setInputPath(dataPath);
      job.setInputFormat(KeyValueTextInputFormat.class);
      job.setInputKeyClass(Text.class);
      job.setInputValueClass(Text.class);

      job.setOutputPath(outPath);
      job.setOutputFormat(SequenceFileOutputFormat.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);

      assertEquals(true, job.waitForCompletion(true));

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // clean-up output
      fs.delete(outPath, true);
      fs.delete(new Path("/tmp/hamatest/parts"), true);
    }
  }
}
