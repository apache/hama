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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public class PartitioningRunner extends
    BSP<Writable, Writable, Writable, Writable, NullWritable> {
  private Configuration conf;
  private int desiredNum;
  private FileSystem fs = null;
  private Path partitionDir;
  private Map<Integer, Map<Writable, Writable>> values = new HashMap<Integer, Map<Writable, Writable>>();

  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {
    this.conf = peer.getConfiguration();
    this.desiredNum = conf.getInt("desired.num.of.tasks", 1);
    this.fs = FileSystem.get(conf);

    Path inputDir = new Path(conf.get("bsp.input.dir"));
    if (fs.isFile(inputDir)) {
      inputDir = inputDir.getParent();
    }

    if(conf.get("bsp.partitioning.dir") != null) {
      this.partitionDir = new Path(conf.get("bsp.partitioning.dir"));
    } else {
      this.partitionDir = new Path(inputDir + "/partitions");
    }
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {
    Partitioner partitioner = getPartitioner();
    KeyValuePair<Writable, Writable> pair = null;

    Class keyClass = null;
    Class valueClass = null;
    while ((pair = peer.readNext()) != null) {
      if (keyClass == null && valueClass == null) {
        keyClass = pair.getKey().getClass();
        valueClass = pair.getValue().getClass();
      }

      int index = Math.abs(partitioner.getPartition(pair.getKey(),
          pair.getValue(), desiredNum));

      if (!values.containsKey(index)) {
        values.put(index, new HashMap<Writable, Writable>());
      }
      values.get(index).put(pair.getKey(), pair.getValue());
    }

    // The reason of use of Memory is to reduce file opens
    for (Map.Entry<Integer, Map<Writable, Writable>> e : values.entrySet()) {
      Path destFile = new Path(partitionDir + "/part-" + e.getKey() + "/file-"
          + peer.getPeerIndex());
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
          destFile, keyClass, valueClass, CompressionType.NONE);
      for (Map.Entry<Writable, Writable> v : e.getValue().entrySet()) {
        writer.append(v.getKey(), v.getValue());
      }
      writer.close();
    }

    peer.sync();

    // merge files into one.
    // TODO if we use header info, we might able to merge files without full scan.
    FileStatus[] status = fs.listStatus(partitionDir);
    for (int j = 0; j < status.length; j++) {
      int idx = Integer.parseInt(status[j].getPath().getName().split("[-]")[1]);
      int assignedID = idx / (desiredNum / peer.getNumPeers());
      if (assignedID == peer.getNumPeers())
        assignedID = assignedID - 1;
      
      // TODO set replica factor to 1. 
      // TODO and check whether we can write to specific DataNode.
      if (assignedID == peer.getPeerIndex()) {
        FileStatus[] files = fs.listStatus(status[j].getPath());
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
            new Path(partitionDir + "/" + getPartitionName(j)), keyClass,
            valueClass, CompressionType.NONE);

        for (int i = 0; i < files.length; i++) {
          SequenceFile.Reader reader = new SequenceFile.Reader(fs,
              files[i].getPath(), conf);

          Writable key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
          Writable value = (Writable) ReflectionUtils.newInstance(valueClass, conf);

          while (reader.next(key, value)) {
            writer.append(key, value);
          }
          reader.close();
        }

        writer.close();
        fs.delete(status[j].getPath(), true);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public Partitioner getPartitioner() {
    return ReflectionUtils.newInstance(conf
        .getClass(Constants.RUNTIME_PARTITIONING_CLASS, HashPartitioner.class,
            Partitioner.class), conf);
  }

  private static String getPartitionName(int i) {
    return "part-" + String.valueOf(100000 + i).substring(1, 6);
  }

}
