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
package org.apache.hama.graph;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.PartitioningRunner.RecordConverter;
import org.apache.hama.util.KeyValuePair;

/**
 * A reader to read Hama's input files and parses a vertex out of it.
 * 
 * 
 * @param <KEYIN> the input format's KEYIN type.
 * @param <VALUEIN> the input format's VALUE_IN type.
 * @param <V> the vertex id type.
 * @param <E> the Edge cost object type.
 * @param <M> the Vertex value/message object type.
 */
@SuppressWarnings("rawtypes")
public abstract class VertexInputReader<KEYIN extends Writable, VALUEIN extends Writable, V extends WritableComparable, E extends Writable, M extends Writable>
    implements RecordConverter {

  private static final Log LOG = LogFactory.getLog(VertexInputReader.class);

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Configuration conf) {
    // initialize the usual vertex structures for read/write methods
    GraphJobRunner.<V, E, M> initClasses(conf);
  }

  private final KeyValuePair<Writable, Writable> outputRecord = new KeyValuePair<Writable, Writable>();

  /**
   * Parses a given key and value into the given vertex. If returned true, the
   * given vertex is considered finished and a new instance will be given in the
   * next call.
   */
  public abstract boolean parseVertex(KEYIN key, VALUEIN value,
      Vertex<V, E, M> vertex) throws Exception;

  @SuppressWarnings("unchecked")
  @Override
  public KeyValuePair<Writable, Writable> convertRecord(
      KeyValuePair<Writable, Writable> inputRecord, Configuration conf) {
    Class<Vertex<V, E, M>> vertexClass = (Class<Vertex<V, E, M>>) conf
        .getClass(GraphJob.VERTEX_CLASS_ATTR, Vertex.class);
    boolean vertexCreation;
    Vertex<V, E, M> vertex = GraphJobRunner
        .<V, E, M> newVertexInstance(vertexClass);
    try {
      vertexCreation = parseVertex((KEYIN) inputRecord.getKey(),
          (VALUEIN) inputRecord.getValue(), vertex);
    } catch (Exception e) {
      LOG.error("Error parsing vertex.", e);
      vertexCreation = false;
    }
    if (!vertexCreation) {
      return null;
    }
    outputRecord.setKey(vertex);
    outputRecord.setValue(NullWritable.get());
    return outputRecord;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int getPartitionId(KeyValuePair<Writable, Writable> inputRecord,
      Partitioner partitioner, Configuration conf, BSPPeer peer, int numTasks) {
    Vertex<V, E, M> vertex = (Vertex<V, E, M>) outputRecord.getKey();
    return Math.abs(partitioner.getPartition(vertex.getVertexID(),
        vertex.getValue(), numTasks));
  }

  // final because we don't want vertices to change ordering
  @Override
  public final Map<Writable, Writable> newMap() {
    return new TreeMap<Writable, Writable>();
  }

}
