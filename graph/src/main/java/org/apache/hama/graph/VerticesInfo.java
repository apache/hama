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

import org.apache.hadoop.io.Writable;

/**
 * VerticesInfo encapsulates the storage of vertices in a BSP Task.
 *
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */

public interface VerticesInfo<V extends Writable, E extends Writable, M extends Writable>
        extends Iterable<Vertex<V, E, M>> {

    /**
     * add a vertex to this storage
     * @param vertex
     */
    public void addVertex(Vertex<V, E, M> vertex);

    /**
     * gives the no. of vertices contained in this storage
     * @return
     */
    public int size();
}
