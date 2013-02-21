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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
  public static final Log LOG = LogFactory.getLog(PartitioningRunner.class);

  private Configuration conf;
  private int desiredNum;
  private FileSystem fs = null;
  private Path partitionDir;
  private RecordConverter converter;
  private Map<Integer, Map<Writable, Writable>> values = new HashMap<Integer, Map<Writable, Writable>>();

  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {

    this.conf = peer.getConfiguration();
    this.desiredNum = conf.getInt(Constants.RUNTIME_DESIRED_PEERS_COUNT, 1);

    this.fs = FileSystem.get(conf);

    Path inputDir = new Path(conf.get(Constants.JOB_INPUT_DIR));
    if (fs.isFile(inputDir)) {
      inputDir = inputDir.getParent();
    }

    converter = ReflectionUtils.newInstance(conf.getClass(
        Constants.RUNTIME_PARTITION_RECORDCONVERTER,
        DefaultRecordConverter.class, RecordConverter.class), conf);
    converter.setup(conf);

    if (conf.get(Constants.RUNTIME_PARTITIONING_DIR) == null) {
      this.partitionDir = new Path(inputDir + "/partitions");
    } else {
      this.partitionDir = new Path(conf.get(Constants.RUNTIME_PARTITIONING_DIR));
    }

  }

  /**
   * This record converter could be used to convert the records from the input
   * format type to the sequential record types the BSP Job uses for
   * computation.
   * 
   */
  public static interface RecordConverter {

    public void setup(Configuration conf);

    /**
     * Should return the Key-Value pair constructed from the input format.
     * 
     * @param inputRecord The input key-value pair.
     * @param conf Configuration of the job.
     * @return the Key-Value pair instance of the expected sequential format.
     *         Should return null if the conversion was not successful.
     */
    public KeyValuePair<Writable, Writable> convertRecord(
        KeyValuePair<Writable, Writable> inputRecord, Configuration conf);

    public int getPartitionId(KeyValuePair<Writable, Writable> inputRecord,
        @SuppressWarnings("rawtypes") Partitioner partitioner,
        Configuration conf, @SuppressWarnings("rawtypes") BSPPeer peer,
        int numTasks);

    /**
     * @return a map implementation, so order can be changed in subclasses if
     *         needed.
     */
    public Map<Writable, Writable> newMap();
  }

  /**
   * The default converter does no conversion.
   */
  public static class DefaultRecordConverter implements RecordConverter {

    @Override
    public KeyValuePair<Writable, Writable> convertRecord(
        KeyValuePair<Writable, Writable> inputRecord, Configuration conf) {
      return inputRecord;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int getPartitionId(KeyValuePair<Writable, Writable> outputRecord,
        @SuppressWarnings("rawtypes") Partitioner partitioner,
        Configuration conf, @SuppressWarnings("rawtypes") BSPPeer peer,
        int numTasks) {
      return Math.abs(partitioner.getPartition(outputRecord.getKey(),
          outputRecord.getValue(), numTasks));
    }

    @Override
    public void setup(Configuration conf) {

    }

    @Override
    public Map<Writable, Writable> newMap() {
      return new HashMap<Writable, Writable>();
    }
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {
    Partitioner partitioner = getPartitioner();
    KeyValuePair<Writable, Writable> pair = null;
    KeyValuePair<Writable, Writable> outputPair = null;

    Class keyClass = null;
    Class valueClass = null;
    Class outputKeyClass = null;
    Class outputValueClass = null;
    while ((pair = peer.readNext()) != null) {
      if (keyClass == null && valueClass == null) {
        keyClass = pair.getKey().getClass();
        valueClass = pair.getValue().getClass();
      }

      outputPair = converter.convertRecord(pair, conf);

      if (outputPair == null) {
        continue;
      }

      if (outputKeyClass == null && outputValueClass == null) {
        outputKeyClass = outputPair.getKey().getClass();
        outputValueClass = outputPair.getValue().getClass();
      }

      int index = converter.getPartitionId(outputPair, partitioner, conf, peer,
          desiredNum);

      Map<Writable, Writable> map = values.get(index);
      if (map == null) {
        map = converter.newMap();
        values.put(index, map);
      }
      map.put(outputPair.getKey(), outputPair.getValue());
    }

    // The reason of use of Memory is to reduce file opens
    for (Map.Entry<Integer, Map<Writable, Writable>> e : values.entrySet()) {
      Path destFile = new Path(partitionDir + "/part-" + e.getKey() + "/file-"
          + peer.getPeerIndex());
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
          destFile, outputKeyClass, outputValueClass, CompressionType.NONE);

      for (Map.Entry<Writable, Writable> v : e.getValue().entrySet()) {
        writer.append(v.getKey(), v.getValue());
      }
      writer.close();
    }

    peer.sync();
    FileStatus[] status = fs.listStatus(partitionDir);
    // Call sync() one more time to avoid concurrent access
    peer.sync();

    // merge files into one.
    // TODO if we use header info, we might able to merge files without full
    // scan.
    for (FileStatus statu : status) {
      int partitionID = Integer
          .parseInt(statu.getPath().getName().split("[-]")[1]);
      int denom = desiredNum / peer.getNumPeers();
      int assignedID = partitionID;
      if (denom > 1) {
        assignedID = partitionID / denom;
      }

      if (assignedID == peer.getNumPeers())
        assignedID = assignedID - 1;

      // TODO set replica factor to 1.
      // TODO and check whether we can write to specific DataNode.
      if (assignedID == peer.getPeerIndex()) {
        Path partitionFile = new Path(partitionDir + "/"
            + getPartitionName(partitionID));

        FileStatus[] files = fs.listStatus(statu.getPath());
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
            partitionFile, outputKeyClass, outputValueClass,
            CompressionType.NONE);

        for (int i = 0; i < files.length; i++) {
          LOG.debug("merge '" + files[i].getPath() + "' into " + partitionDir
              + "/" + getPartitionName(partitionID));

          SequenceFile.Reader reader = new SequenceFile.Reader(fs,
              files[i].getPath(), conf);

          Writable key = (Writable) ReflectionUtils.newInstance(outputKeyClass,
              conf);
          Writable value = (Writable) ReflectionUtils.newInstance(
              outputValueClass, conf);

          while (reader.next(key, value)) {
            writer.append(key, value);
          }
          reader.close();
        }

        writer.close();
        fs.delete(statu.getPath(), true);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public Partitioner getPartitioner() {
    return ReflectionUtils.newInstance(conf.getClass(
        Constants.RUNTIME_PARTITIONING_CLASS, HashPartitioner.class,
        Partitioner.class), conf);
  }

  private static String getPartitionName(int i) {
    return "part-" + String.valueOf(100000 + i).substring(1, 6);
  }

}
