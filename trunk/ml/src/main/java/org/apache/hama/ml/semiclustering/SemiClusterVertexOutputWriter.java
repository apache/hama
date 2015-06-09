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

package org.apache.hama.ml.semiclustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.graph.GraphJobMessage;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexOutputWriter;

import java.io.IOException;

/**
 * The VertexOutputWriter defines what parts of the vertex shall be written to
 * the output format.
 * 
 * @param <V> the vertexID type.
 * @param <E> the edge value type.
 * @param <M> the vertex value type.
 */
@SuppressWarnings("rawtypes")
public class SemiClusterVertexOutputWriter<KEYOUT extends Writable, VALUEOUT extends Writable, V extends WritableComparable, E extends Writable, M extends Writable>
    implements VertexOutputWriter<KEYOUT, VALUEOUT, V, E, M> {

  @Override
  public void setup(Configuration conf) {
    // do nothing
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Vertex<V, E, M> vertex,
      BSPPeer<Writable, Writable, KEYOUT, VALUEOUT, GraphJobMessage> peer)
      throws IOException {
    SemiClusterMessage vertexValue = (SemiClusterMessage) vertex.getValue();
    peer.write((KEYOUT) vertex.getVertexID(), (VALUEOUT) new Text(vertexValue
        .getSemiClusterContainThis().toString()));
  }

}
