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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.graph.IDSkippingIterator.Strategy;

/**
 * Stores the sorted vertices into a local file. It doesn't allow modification
 * and random access by vertexID.
 * 
 * @param <V>
 * @param <E>
 * @param <M>
 */
@SuppressWarnings("rawtypes")
public final class DiskVerticesInfo<V extends WritableComparable, E extends Writable, M extends Writable>
    implements VerticesInfo<V, E, M> {

  public static final String DISK_VERTICES_PATH_KEY = "hama.disk.vertices.path";

  private static final byte NULL = 0;
  private static final byte NOT_NULL = 1;

  private FSDataOutputStream staticGraphPartsDos;
  private FSDataInputStream staticGraphPartsDis;

  private FSDataOutputStream softGraphPartsDos;
  private FSDataInputStream softGraphPartsDis;

  private FSDataOutputStream softGraphPartsNextIterationDos;

  private BitSet activeVertices;
  private long[] softValueOffsets;
  private long[] softValueOffsetsNextIteration;
  private long[] staticOffsets;

  private ArrayList<Long> tmpSoftOffsets;
  private ArrayList<Long> tmpStaticOffsets;

  private int size;
  private boolean lockedAdditions = false;
  private String rootPath;
  private Vertex<V, E, M> cachedVertexInstance;
  private int currentStep = 0;
  private int index = 0;
  private HamaConfiguration conf;
  private GraphJobRunner<V, E, M> runner;
  private String staticFile;

  @Override
  public void init(GraphJobRunner<V, E, M> runner, HamaConfiguration conf,
      TaskAttemptID attempt) throws IOException {
    this.runner = runner;
    this.conf = conf;
    tmpSoftOffsets = new ArrayList<Long>();
    tmpStaticOffsets = new ArrayList<Long>();
    String p = conf.get(DISK_VERTICES_PATH_KEY, "/tmp/graph/");
    rootPath = p + attempt.getJobID().toString() + "/" + attempt.toString()
        + "/";
    LocalFileSystem local = FileSystem.getLocal(conf);
    local.mkdirs(new Path(rootPath));
    staticFile = rootPath + "static.graph";
    local.delete(new Path(staticFile), false);
    staticGraphPartsDos = local.create(new Path(staticFile));
    String softGraphFileName = getSoftGraphFileName(rootPath, currentStep);
    local.delete(new Path(softGraphFileName), false);
    softGraphPartsDos = local.create(new Path(softGraphFileName));
  }

  @Override
  public void cleanup(HamaConfiguration conf, TaskAttemptID attempt)
      throws IOException {
    IOUtils.cleanup(null, softGraphPartsDos, softGraphPartsNextIterationDos,
        staticGraphPartsDis, softGraphPartsDis);
    // delete the contents
    FileSystem.getLocal(conf).delete(new Path(rootPath), true);
  }

  @Override
  public void addVertex(Vertex<V, E, M> vertex) throws IOException {
    // messages must be added in sorted order to work this out correctly
    checkArgument(!lockedAdditions,
        "Additions are locked now, nobody is allowed to change the structure anymore.");

    // write the static parts
    tmpStaticOffsets.add(staticGraphPartsDos.getPos());
    vertex.getVertexID().write(staticGraphPartsDos);
    staticGraphPartsDos.writeInt(vertex.getEdges() == null ? 0 : vertex
        .getEdges().size());
    for (Edge<?, ?> e : vertex.getEdges()) {
      e.getDestinationVertexID().write(staticGraphPartsDos);
    }

    serializeSoft(vertex, -1, null, softGraphPartsDos);

    size++;
  }

  @Override
  public void removeVertex(V vertexID) {
    throw new UnsupportedOperationException(
        "DiskVerticesInfo doesn't support this operation. Please use the MapVerticesInfo.");
  }

  /**
   * Serializes the vertex's soft parts to its file. If the vertex does not have
   * an index yet (e.G. at startup) you can provide -1 and it will be added to
   * the temporary storage.
   */
  private void serializeSoft(Vertex<V, E, M> vertex, int index,
      long[] softValueOffsets, FSDataOutputStream softGraphParts)
      throws IOException {
    // safe offset write the soft parts
    if (index >= 0) {
      softValueOffsets[index] = softGraphParts.getPos();
      // only set the bitset if we've finished the setup
      activeVertices.set(index, vertex.isHalted());
    } else {
      tmpSoftOffsets.add(softGraphParts.getPos());
    }
    if (vertex.getValue() == null) {
      softGraphParts.write(NULL);
    } else {
      softGraphParts.write(NOT_NULL);
      vertex.getValue().write(softGraphParts);
    }
    vertex.writeState(softGraphParts);
    softGraphParts.writeInt(vertex.getEdges().size());
    for (Edge<?, ?> e : vertex.getEdges()) {
      if (e.getValue() == null) {
        softGraphParts.write(NULL);
      } else {
        softGraphParts.write(NOT_NULL);
        e.getValue().write(softGraphParts);
      }
    }
  }

  @Override
  public void finishAdditions() {
    // copy the arraylist to a plain array
    softValueOffsets = copy(tmpSoftOffsets);
    softValueOffsetsNextIteration = copy(tmpSoftOffsets);
    staticOffsets = copy(tmpStaticOffsets);
    activeVertices = new BitSet(size);

    tmpStaticOffsets = null;
    tmpSoftOffsets = null;
    IOUtils.cleanup(null, staticGraphPartsDos, softGraphPartsDos);
    // prevent additional vertices from beeing added
    lockedAdditions = true;
  }

  @Override
  public void finishRemovals() {
    throw new UnsupportedOperationException(
        "DiskVerticesInfo doesn't support this operation. Please use the MapVerticesInfo.");
  }

  private static long[] copy(ArrayList<Long> lst) {
    long[] arr = new long[lst.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = lst.get(i);
    }
    return arr;
  }

  @Override
  public void startSuperstep() throws IOException {
    index = 0;
    String softGraphFileName = getSoftGraphFileName(rootPath, currentStep);
    LocalFileSystem local = FileSystem.getLocal(conf);
    local.delete(new Path(softGraphFileName), true);
    softGraphPartsNextIterationDos = local.create(new Path(softGraphFileName));
    softValueOffsets = softValueOffsetsNextIteration;
    softValueOffsetsNextIteration = new long[softValueOffsetsNextIteration.length];
  }

  @Override
  public void finishVertexComputation(Vertex<V, E, M> vertex)
      throws IOException {
    // write to the soft parts
    serializeSoft(vertex, index++, softValueOffsetsNextIteration,
        softGraphPartsNextIterationDos);
  }

  @Override
  public void finishSuperstep() throws IOException {
    // do not delete files in the first step
    IOUtils.cleanup(null, softGraphPartsDos, softGraphPartsNextIterationDos,
        softGraphPartsDis);
    if (currentStep > 0) {
      LocalFileSystem local = FileSystem.getLocal(conf);
      local.delete(new Path(getSoftGraphFileName(rootPath, currentStep - 1)),
          true);
      String softGraphFileName = getSoftGraphFileName(rootPath, currentStep);
      softGraphPartsDis = local.open(new Path(softGraphFileName));
    }
    currentStep++;
  }

  @Override
  public int size() {
    return size;
  }

  private final class IDSkippingDiskIterator extends
      IDSkippingIterator<V, E, M> {

    int currentIndex = 0;

    @Override
    public Vertex<V, E, M> next() {
      return cachedVertexInstance;
    }

    @Override
    public boolean hasNext(V e,
        org.apache.hama.graph.IDSkippingIterator.Strategy strat) {
      if (currentIndex >= size) {
        return false;
      } else {
        currentIndex = fill(strat, currentIndex, e);
        return true;
      }
    }

  }

  @Override
  public IDSkippingIterator<V, E, M> skippingIterator() {
    try {
      // reset
      String softGraphFileName = getSoftGraphFileName(rootPath,
          Math.max(0, currentStep - 1));
      LocalFileSystem local = FileSystem.getLocal(conf);
      // close the files
      IOUtils.cleanup(null, softGraphPartsDos, softGraphPartsDis,
          staticGraphPartsDis, staticGraphPartsDos);
      softGraphPartsDis = local.open(new Path(softGraphFileName));
      staticGraphPartsDis = local.open(new Path(staticFile));

      // ensure the vertex is not null
      if (cachedVertexInstance == null) {
        cachedVertexInstance = GraphJobRunner
            .<V, E, M> newVertexInstance(GraphJobRunner.VERTEX_CLASS);
        cachedVertexInstance.setRunner(runner);
      }
      ensureVertexIDNotNull();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new IDSkippingDiskIterator();
  }

  @SuppressWarnings("unchecked")
  private void ensureVertexIDNotNull() {
    if (cachedVertexInstance.getVertexID() == null) {
      cachedVertexInstance.setVertexID((V) GraphJobRunner
          .createVertexIDObject());
    }
  }

  @SuppressWarnings("unchecked")
  private void ensureVertexValueNotNull() {
    if (cachedVertexInstance.getValue() == null) {
      cachedVertexInstance.setValue((M) GraphJobRunner.createVertexValue());
    }
  }

  @SuppressWarnings({ "unchecked", "static-method" })
  private void ensureEdgeIDNotNull(Edge<V, E> edge) {
    if (edge.getDestinationVertexID() == null) {
      edge.setDestinationVertexID((V) GraphJobRunner.createVertexIDObject());
    }
  }

  @SuppressWarnings({ "unchecked", "static-method" })
  private void ensureEdgeValueNotNull(Edge<V, E> edge) {
    if (edge.getValue() == null) {
      edge.setValue((E) GraphJobRunner.createEdgeCostObject());
    }
  }

  /**
   * Fills the cachedVertexInstance with the next acceptable item after the
   * given index that matches the given messageVertexID if provided.
   * 
   * @param strat the strategy that defines if a vertex that is serialized
   *          should be accepted.
   * @param index the index of the vertices to start from.
   * @param messageVertexId the message vertex id that can be matched by the
   *          strategy. Can be null as well, this is handled by the strategy.
   * @return the index of the item after the currently found item.
   */
  private int fill(Strategy strat, int index, V messageVertexId) {
    try {
      while (true) {
        // seek until we found something that satisfied our strategy
        staticGraphPartsDis.seek(staticOffsets[index]);
        boolean halted = activeVertices.get(index);
        cachedVertexInstance.setVotedToHalt(halted);
        cachedVertexInstance.getVertexID().readFields(staticGraphPartsDis);
        if (strat.accept(cachedVertexInstance, messageVertexId)) {
          break;
        }
        if (++index >= size) {
          return size;
        }
      }
      softGraphPartsDis.seek(softValueOffsets[index]);

      // setting vertex value null here, because it may be overridden. Messaging
      // is not materializing the message directly- so it is possible for the
      // read fields method to change this object (thus a new object).
      cachedVertexInstance.setValue(null);
      if (softGraphPartsDis.readByte() == NOT_NULL) {
        ensureVertexValueNotNull();
        cachedVertexInstance.getValue().readFields(softGraphPartsDis);
      }

      cachedVertexInstance.readState(softGraphPartsDis);
      int numEdges = staticGraphPartsDis.readInt();
      int softEdges = softGraphPartsDis.readInt();
      if (softEdges != numEdges) {
        throw new IllegalArgumentException(
            "Number of edges seemed to change. This is not possible (yet).");
      }
      // edges could actually be cached, however the local mode is preventing it
      // sometimes as edge destinations are send and possible overridden in
      // messages here.
      ArrayList<Edge<V, E>> edges = new ArrayList<Edge<V, E>>();
      // read the soft file in parallel
      for (int i = 0; i < numEdges; i++) {
        Edge<V, E> edge = new Edge<V, E>();
        ensureEdgeValueNotNull(edge);
        ensureEdgeIDNotNull(edge);
        edge.getDestinationVertexID().readFields(staticGraphPartsDis);
        if (softGraphPartsDis.readByte() == NOT_NULL) {
          ensureEdgeValueNotNull(edge);
          edge.getValue().readFields(softGraphPartsDis);
        } else {
          edge.setValue(null);
        }
        edges.add(edge);
      }

      // make edges unmodifiable
      cachedVertexInstance.setEdges(Collections.unmodifiableList(edges));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return index + 1;
  }

  private static String getSoftGraphFileName(String root, int step) {
    return root + "soft_" + step + ".graph";
  }
}
