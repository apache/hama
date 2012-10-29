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
 * A reader to read Hama's input files and parses a vertex out of it.
 */
public abstract class VertexInputReader<KEYIN extends Writable, VALUEIN extends Writable, V extends Writable, E extends Writable, M extends Writable> {

  /**
   * Parses a given key and value into the given vertex. If returned true, the
   * given vertex is considered finished and a new instance will be given in the
   * next call.
   */
  public abstract boolean parseVertex(KEYIN key, VALUEIN value,
      Vertex<V, E, M> vertex) throws Exception;

}
