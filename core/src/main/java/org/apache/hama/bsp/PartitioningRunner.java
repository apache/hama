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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
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
     * @throws IOException
     */
    public KeyValuePair<Writable, Writable> convertRecord(
        KeyValuePair<Writable, Writable> inputRecord, Configuration conf)
        throws IOException;

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

  @Override
  @SuppressWarnings({ "rawtypes" })
  public void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, NullWritable> peer)
      throws IOException, SyncException, InterruptedException {

    int peerNum = peer.getNumPeers();
    Partitioner partitioner = getPartitioner();
    KeyValuePair<Writable, Writable> rawRecord = null;
    KeyValuePair<Writable, Writable> convertedRecord = null;

    Class convertedKeyClass = null;
    Class rawKeyClass = null;
    Class rawValueClass = null;
    MapWritable raw = null;

    while ((rawRecord = peer.readNext()) != null) {
      if (rawKeyClass == null && rawValueClass == null) {
        rawKeyClass = rawRecord.getKey().getClass();
        rawValueClass = rawRecord.getValue().getClass();
      }
      convertedRecord = converter.convertRecord(rawRecord, conf);

      if (convertedRecord == null) {
        throw new IOException("The converted record can't be null.");
      }

      Writable convertedKey = convertedRecord.getKey();
      convertedKeyClass = convertedKey.getClass();

      int index = converter.getPartitionId(convertedRecord, partitioner, conf,
          peer, desiredNum);

      if (!writerCache.containsKey(index)) {
        Path destFile = new Path(partitionDir + "/part-" + index + "/file-"
            + peer.getPeerIndex());
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
            destFile, convertedKeyClass, MapWritable.class,
            CompressionType.NONE);
        writerCache.put(index, writer);
      }

      raw = new MapWritable();
      raw.put(rawRecord.getKey(), rawRecord.getValue());

      writerCache.get(index).append(convertedKey, raw);
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
        if (convertedRecord.getKey() instanceof WritableComparable
            && conf.getBoolean(Constants.PARTITION_SORT_BY_KEY, false)) {
          mergeSortedFiles(files, destinationFilePath, convertedKeyClass,
              rawKeyClass, rawValueClass);
        } else {
          mergeFiles(files, destinationFilePath, convertedKeyClass,
              rawKeyClass, rawValueClass);
        }
        fs.delete(stat.getPath(), true);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public Map<Integer, KeyValuePair<WritableComparable, MapWritable>> candidates = new HashMap<Integer, KeyValuePair<WritableComparable, MapWritable>>();

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void mergeSortedFiles(FileStatus[] status, Path destinationFilePath,
      Class convertedKeyClass, Class rawKeyClass, Class rawValueClass)
      throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
        destinationFilePath, rawKeyClass, rawValueClass, CompressionType.NONE);
    WritableComparable convertedKey;
    MapWritable value;

    Map<Integer, SequenceFile.Reader> readers = new HashMap<Integer, SequenceFile.Reader>();
    for (int i = 0; i < status.length; i++) {
      SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
          convertedKeyClass, MapWritable.class, conf);
      sorter.setMemory(conf
          .getInt("bsp.input.runtime.partitioning.sort.mb", 50) * 1024 * 1024);
      sorter.setFactor(conf.getInt(
          "bsp.input.runtime.partitioning.sort.factor", 10));
      sorter.sort(status[i].getPath(), status[i].getPath().suffix(".sorted"));

      readers.put(i,
          new SequenceFile.Reader(fs, status[i].getPath().suffix(".sorted"),
              conf));
    }

    for (int i = 0; i < readers.size(); i++) {
      convertedKey = (WritableComparable) ReflectionUtils.newInstance(
          convertedKeyClass, conf);
      value = new MapWritable();

      readers.get(i).next(convertedKey, value);
      candidates.put(i, new KeyValuePair(convertedKey, value));
    }

    while (readers.size() > 0) {
      convertedKey = (WritableComparable) ReflectionUtils.newInstance(
          convertedKeyClass, conf);
      value = new MapWritable();

      int readerIndex = 0;
      WritableComparable firstKey = null;
      MapWritable rawRecord = null;

      for (Map.Entry<Integer, KeyValuePair<WritableComparable, MapWritable>> keys : candidates
          .entrySet()) {
        if (firstKey == null) {
          readerIndex = keys.getKey();
          firstKey = keys.getValue().getKey();
          rawRecord = (MapWritable) keys.getValue().getValue();
        } else {
          WritableComparable currentKey = keys.getValue().getKey();
          if (firstKey.compareTo(currentKey) > 0) {
            readerIndex = keys.getKey();
            firstKey = currentKey;
            rawRecord = (MapWritable) keys.getValue().getValue();
          }
        }
      }

      for (Map.Entry<Writable, Writable> e : rawRecord.entrySet()) {
        writer.append(e.getKey(), e.getValue());
      }

      candidates.remove(readerIndex);

      if (readers.get(readerIndex).next(convertedKey, value)) {
        candidates.put(readerIndex, new KeyValuePair(convertedKey, value));
      } else {
        readers.get(readerIndex).close();
        readers.remove(readerIndex);
      }
    }

    candidates.clear();
    writer.close();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void mergeFiles(FileStatus[] status, Path destinationFilePath,
      Class convertedKeyClass, Class rawKeyClass, Class rawValueClass)
      throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
        destinationFilePath, rawKeyClass, rawValueClass, CompressionType.NONE);
    Writable key;
    MapWritable rawRecord;

    for (int i = 0; i < status.length; i++) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs,
          status[i].getPath(), conf);
      key = (Writable) ReflectionUtils.newInstance(convertedKeyClass, conf);
      rawRecord = new MapWritable();

      while (reader.next(key, rawRecord)) {
        for (Map.Entry<Writable, Writable> e : rawRecord.entrySet()) {
          writer.append(e.getKey(), e.getValue());
        }
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
