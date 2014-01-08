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
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.pipes.PipesPartitioner;

public class PartitioningRunner extends
    BSP<Writable, Writable, Writable, Writable, NullWritable> {
  public static final Log LOG = LogFactory.getLog(PartitioningRunner.class);

  private Configuration conf;
  private int desiredNum;
  private FileSystem fs = null;
  private Path partitionDir;
  private RecordConverter converter;
  private PipesPartitioner<?, ?> pipesPartitioner = null;

  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {

    this.conf = peer.getConfiguration();
    this.desiredNum = conf.getInt(Constants.RUNTIME_DESIRED_PEERS_COUNT, 1);

    this.fs = FileSystem.get(conf);

    converter = ReflectionUtils.newInstance(conf.getClass(
        Constants.RUNTIME_PARTITION_RECORDCONVERTER,
        DefaultRecordConverter.class, RecordConverter.class), conf);
    converter.setup(conf);

    if (conf.get(Constants.RUNTIME_PARTITIONING_DIR) == null) {
      this.partitionDir = new Path(conf.get("bsp.output.dir"));
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
        @SuppressWarnings("rawtypes")
        Partitioner partitioner, Configuration conf,
        @SuppressWarnings("rawtypes")
        BSPPeer peer, int numTasks);
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
        @SuppressWarnings("rawtypes")
        Partitioner partitioner, Configuration conf,
        @SuppressWarnings("rawtypes")
        BSPPeer peer, int numTasks) {
      return Math.abs(partitioner.getPartition(outputRecord.getKey(),
          outputRecord.getValue(), numTasks));
    }

    @Override
    public void setup(Configuration conf) {
    }

  }

  public Map<Integer, SequenceFile.Writer> writerCache = new HashMap<Integer, SequenceFile.Writer>();

  @SuppressWarnings("rawtypes")
  public SortedMap<WritableComparable, KeyValuePair<IntWritable, KeyValuePair>> sortedMap = new TreeMap<WritableComparable, KeyValuePair<IntWritable, KeyValuePair>>();

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {

    int peerNum = peer.getNumPeers();
    Partitioner partitioner = getPartitioner();
    KeyValuePair<Writable, Writable> pair = null;
    KeyValuePair<Writable, Writable> outputPair = null;

    Class keyClass = null;
    Class valueClass = null;
    while ((pair = peer.readNext()) != null) {
      if (keyClass == null && valueClass == null) {
        keyClass = pair.getKey().getClass();
        valueClass = pair.getValue().getClass();
      }
      outputPair = converter.convertRecord(pair, conf);

      if (outputPair == null) {
        continue;
      }

      int index = converter.getPartitionId(outputPair, partitioner, conf, peer,
          desiredNum);

      // if key is comparable and it need to be sorted by key,
      if (outputPair.getKey() instanceof WritableComparable
          && conf.getBoolean(Constants.PARTITION_SORT_BY_KEY, false)) {
        sortedMap.put(
            (WritableComparable) outputPair.getKey(),
            new KeyValuePair(new IntWritable(index), new KeyValuePair(pair
                .getKey(), pair.getValue())));
      } else {
        if (!writerCache.containsKey(index)) {
          Path destFile = new Path(partitionDir + "/part-" + index + "/file-"
              + peer.getPeerIndex());
          SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
              destFile, keyClass, valueClass, CompressionType.NONE);
          writerCache.put(index, writer);
        }

        writerCache.get(index).append(pair.getKey(), pair.getValue());
      }
    }

    if (sortedMap.size() > 0) {
      writeSortedFile(peer.getPeerIndex(), keyClass, valueClass);
    }

    for (SequenceFile.Writer w : writerCache.values()) {
      w.close();
    }

    peer.sync();
    FileStatus[] status = fs.listStatus(partitionDir);
    // Call sync() one more time to avoid concurrent access
    peer.sync();

    for (FileStatus stat : status) {
      int partitionID = Integer
          .parseInt(stat.getPath().getName().split("[-]")[1]);

      if (getMergeProcessorID(partitionID, peerNum) == peer.getPeerIndex()) {
        Path destinationFilePath = new Path(partitionDir + "/"
            + getPartitionName(partitionID));

        FileStatus[] files = fs.listStatus(stat.getPath());
        if (outputPair.getKey() instanceof WritableComparable
            && conf.getBoolean(Constants.PARTITION_SORT_BY_KEY, false)) {
          mergeSortedFiles(files, destinationFilePath, keyClass, valueClass);
        } else {
          mergeFiles(files, destinationFilePath, keyClass, valueClass);
        }
        fs.delete(stat.getPath(), true);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private void writeSortedFile(int peerIndex, Class keyClass, Class valueClass)
      throws IOException {
    for (Entry<WritableComparable, KeyValuePair<IntWritable, KeyValuePair>> e : sortedMap
        .entrySet()) {
      int index = ((IntWritable) e.getValue().getKey()).get();
      KeyValuePair rawRecord = e.getValue().getValue();

      if (!writerCache.containsKey(index)) {
        Path destFile = new Path(partitionDir + "/part-" + index + "/file-"
            + peerIndex);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
            destFile, keyClass, valueClass, CompressionType.NONE);
        writerCache.put(index, writer);
      }

      writerCache.get(index).append(rawRecord.getKey(), rawRecord.getValue());
    }

    sortedMap.clear();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void mergeSortedFiles(FileStatus[] status, Path destinationFilePath,
      Class keyClass, Class valueClass) throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
        destinationFilePath, keyClass, valueClass, CompressionType.NONE);
    KeyValuePair outputPair = null;
    Writable key;
    Writable value;

    Map<Integer, SequenceFile.Reader> readers = new HashMap<Integer, SequenceFile.Reader>();
    for (int i = 0; i < status.length; i++) {
      readers.put(i, new SequenceFile.Reader(fs, status[i].getPath(), conf));
    }

    for (int i = 0; i < readers.size(); i++) {
      key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
      value = (Writable) ReflectionUtils.newInstance(valueClass, conf);

      readers.get(i).next(key, value);
      KeyValuePair record = new KeyValuePair(key, value);
      outputPair = converter.convertRecord(record, conf);
      sortedMap.put((WritableComparable) outputPair.getKey(), new KeyValuePair(
          new IntWritable(i), record));
    }

    while (readers.size() > 0) {
      key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
      value = (Writable) ReflectionUtils.newInstance(valueClass, conf);

      WritableComparable firstKey = sortedMap.firstKey();
      KeyValuePair kv = sortedMap.get(firstKey);
      int readerIndex = ((IntWritable) kv.getKey()).get();
      KeyValuePair rawRecord = (KeyValuePair) kv.getValue();
      writer.append(rawRecord.getKey(), rawRecord.getValue());

      sortedMap.remove(firstKey);

      if (readers.get(readerIndex).next(key, value)) {
        KeyValuePair record = new KeyValuePair(key, value);
        outputPair = converter.convertRecord(record, conf);
        sortedMap.put((WritableComparable) outputPair.getKey(),
            new KeyValuePair(new IntWritable(readerIndex), record));
      } else {
        readers.get(readerIndex).close();
        readers.remove(readerIndex);
      }
    }

    sortedMap.clear();
    writer.close();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void mergeFiles(FileStatus[] status, Path destinationFilePath,
      Class keyClass, Class valueClass) throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
        destinationFilePath, keyClass, valueClass, CompressionType.NONE);
    Writable key;
    Writable value;

    for (int i = 0; i < status.length; i++) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs,
          status[i].getPath(), conf);
      key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
      value = (Writable) ReflectionUtils.newInstance(valueClass, conf);

      while (reader.next(key, value)) {
        writer.append(key, value);
      }
      reader.close();
    }
    writer.close();
  }

  @Override
  public void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException {
    if (this.pipesPartitioner != null) {
      this.pipesPartitioner.cleanup();
    }
  }

  public static int getMergeProcessorID(int partitionID, int peerNum) {
    return partitionID % peerNum;
  }

  @SuppressWarnings("rawtypes")
  public Partitioner getPartitioner() {
    Class<? extends Partitioner> partitionerClass = conf.getClass(
        Constants.RUNTIME_PARTITIONING_CLASS, HashPartitioner.class,
        Partitioner.class);

    LOG.debug(Constants.RUNTIME_PARTITIONING_CLASS + ": "
        + partitionerClass.toString());

    // Check for Hama Pipes Partitioner
    Partitioner partitioner = null;
    if (PipesPartitioner.class.equals(partitionerClass)) {
      try {
        Constructor<? extends Partitioner> ctor = partitionerClass
            .getConstructor(Configuration.class);
        partitioner = ctor.newInstance(conf);
        this.pipesPartitioner = (PipesPartitioner) partitioner;
      } catch (Exception e) {
        LOG.error(e);
      }
    } else {
      partitioner = ReflectionUtils.newInstance(partitionerClass, conf);
    }
    return partitioner;
  }

  private static String getPartitionName(int i) {
    return "part-" + String.valueOf(100000 + i).substring(1, 6);
  }

}
