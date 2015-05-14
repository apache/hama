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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.util.KeyValuePair;
import org.apache.hama.pipes.PipesPartitioner;

public class PartitioningRunner extends
    BSP<Writable, Writable, Writable, Writable, MapWritable> {
  public static final Log LOG = LogFactory.getLog(PartitioningRunner.class);

  private Configuration conf;
  private RecordConverter converter;
  private PipesPartitioner<?, ?> pipesPartitioner = null;

  @Override
  public final void setup(
      BSPPeer<Writable, Writable, Writable, Writable, MapWritable> peer)
      throws IOException, SyncException, InterruptedException {

    this.conf = peer.getConfiguration();

    converter = ReflectionUtils.newInstance(conf.getClass(
        Constants.RUNTIME_PARTITION_RECORDCONVERTER,
        DefaultRecordConverter.class, RecordConverter.class), conf);
    converter.setup(conf);
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
        @SuppressWarnings("rawtypes") Partitioner partitioner,
        Configuration conf, @SuppressWarnings("rawtypes") BSPPeer peer,
        int numTasks);
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

  }

  public Map<Integer, SequenceFile.Writer> writerCache = new HashMap<Integer, SequenceFile.Writer>();

  @Override
  @SuppressWarnings({ "rawtypes" })
  public void bsp(
      BSPPeer<Writable, Writable, Writable, Writable, MapWritable> peer)
      throws IOException, SyncException, InterruptedException {

    Partitioner partitioner = getPartitioner();
    KeyValuePair<Writable, Writable> rawRecord = null;
    KeyValuePair<Writable, Writable> convertedRecord = null;

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

      int index = converter.getPartitionId(convertedRecord, partitioner, conf,
          peer, peer.getNumPeers());

      raw = new MapWritable();
      raw.put(rawRecord.getKey(), rawRecord.getValue());
      
      peer.send(peer.getPeerName(index), raw);
    }

    peer.sync();

    MapWritable record;

    while ((record = peer.getCurrentMessage()) != null) {
      for (Map.Entry<Writable, Writable> e : record.entrySet()) {
        peer.write(e.getKey(), e.getValue());
      }
    }

  }

  @Override
  public void cleanup(
      BSPPeer<Writable, Writable, Writable, Writable, MapWritable> peer)
      throws IOException {
    if (this.pipesPartitioner != null) {
      this.pipesPartitioner.cleanup();
    }
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

}
