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

import java.io.IOException;
import java.util.Iterator;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.utils.CacheValuesIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * An off heap version of a {@link Vertex} storage.
 */
public class OffHeapVerticesInfo<V extends WritableComparable, E extends Writable, M extends Writable>
        implements VerticesInfo<V, E, M> {

    public static final String DM_STRICT_ITERATOR = "dm.iterator.strict";
    public static final String DM_BUFFERS = "dm.buffers";
    public static final String DM_SIZE = "dm.size";
    public static final String DM_CAPACITY = "dm.capacity";
    public static final String DM_CONCURRENCY = "dm.concurrency";
    public static final String DM_DISPOSAL_TIME = "dm.disposal.time";
    public static final String DM_SERIALIZER = "dm.serializer";

    private CacheService<V, Vertex<V, E, M>> vertices;

    private boolean strict;

    @Override
    public void init(GraphJobRunner<V, E, M> runner, Configuration conf, TaskAttemptID attempt) throws IOException {
        this.strict = conf.getBoolean(DM_STRICT_ITERATOR, true);
        this.vertices = new DirectMemory<V, Vertex<V, E, M>>()
                .setNumberOfBuffers(conf.getInt(DM_BUFFERS, 10))
                .setSize(conf.getInt(DM_SIZE, 102400))
                .setInitialCapacity(conf.getInt(DM_CAPACITY, 1000))
                .setConcurrencyLevel(conf.getInt(DM_CONCURRENCY, 10))
                .setDisposalTime(conf.getInt(DM_DISPOSAL_TIME, 360000))
                .newCacheService();
    }

    @Override
    public void cleanup(Configuration conf, TaskAttemptID attempt) throws IOException {
    }

    public void addVertex(Vertex<V, E, M> vertex) {
        vertices.put(vertex.getVertexID(), vertex);
    }

    @Override
    public void finishAdditions() {
    }

    @Override
    public void startSuperstep() throws IOException {
    }

    @Override
    public void finishSuperstep() throws IOException {
    }

    @Override
    public void finishVertexComputation(Vertex<V, E, M> vertex) throws IOException {
    }

    @Override
    public boolean isFinishedAdditions() {
        return false;
    }

    public void clear() {
        vertices.clear();
    }

    public int size() {
        return (int) this.vertices.entries();
    }

    @Override
    public IDSkippingIterator<V, E, M> skippingIterator() {
        final Iterator<Vertex<V, E, M>> vertexIterator =
                new CacheValuesIterable<V, Vertex<V, E, M>>(vertices, strict).iterator();
        return new IDSkippingIterator<V, E, M>() {
            @Override
            public boolean hasNext(V e, Strategy strat) {
                return vertexIterator.hasNext();
            }

            @Override
            public Vertex<V, E, M> next() {
                return vertexIterator.next();
            }
        };
    }

}
