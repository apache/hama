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

import java.util.Iterator;

import org.apache.directmemory.DirectMemory;
import org.apache.directmemory.cache.CacheService;
import org.apache.directmemory.utils.CacheValuesIterable;
import org.apache.hadoop.io.Writable;

/**
 * An off heap version of a {@link Vertex} storage.
 */
public class OffHeapVerticesInfo<V extends Writable, E extends Writable, M extends Writable>
        implements Iterable<Vertex<V, E, M>> {

    private final CacheService<V, Vertex<V, E, M>> vertices;

    private final boolean strict;

    public OffHeapVerticesInfo(boolean strict) {
        this.strict = strict;
        this.vertices = new DirectMemory<V, Vertex<V, E, M>>().setNumberOfBuffers(1).
                setSize(1000).setInitialCapacity(10000).setConcurrencyLevel(100).
                setDisposalTime(100000).newCacheService();
    }

    public OffHeapVerticesInfo() {
        this(true);
    }

    public void addVertex(Vertex<V, E, M> vertex) {
        vertices.put(vertex.getVertexID(), vertex);
    }

    public void clear() {
        vertices.clear();
    }

    public int size() {
        return (int) this.vertices.entries();
    }

    @Override
    public Iterator<Vertex<V, E, M>> iterator() {
        return new CacheValuesIterable<V, Vertex<V, E, M>>(vertices, strict).iterator();
    }

}
