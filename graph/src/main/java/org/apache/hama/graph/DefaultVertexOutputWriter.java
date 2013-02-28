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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;

/**
 * The VertexOutputWriter defines what parts of the vertex shall be written to
 * the output format.
 * 
 * @param <V> the vertexID type.
 * @param <E> the edge value type.
 * @param <M> the vertex value type.
 */
@SuppressWarnings("rawtypes")
public class DefaultVertexOutputWriter<V extends WritableComparable, E extends Writable, M extends Writable>
    implements VertexOutputWriter<V, M, V, E, M> {

  @Override
  public void setup(Configuration conf) {
    // do nothing
  }

  @Override
  public void write(Vertex<V, E, M> vertex,
      BSPPeer<Writable, Writable, V, M, GraphJobMessage> peer)
      throws IOException {
    peer.write(vertex.getVertexID(), vertex.getValue());
  }

}
