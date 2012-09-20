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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.WritableSerialization;

import com.google.common.base.Preconditions;

/**
 * Writable serialization for Hadoop objects with a class cache to reduce the
 * amount of writing the classname instead of a single integer (with V
 * compression, so most of the time it just takes a single byte). <br/>
 * Enhanced by graph instance that can be passed.
 */
public final class VertexWritableSerialization<K extends Writable> extends
    WritableSerialization<K> {

  private static final long serialVersionUID = 1L;
  @SuppressWarnings("rawtypes")
  private GraphJobRunner runner;

  public VertexWritableSerialization() {
  }

  public VertexWritableSerialization(Class<?> writableClazz,
      @SuppressWarnings("rawtypes") GraphJobRunner runner) {
    super(writableClazz);
    Preconditions
        .checkArgument(
            Vertex.class.isAssignableFrom(writableClazz),
            "Class "
                + writableClazz
                + " is not assignable from Vertex class! This class only serializes vertices!");
    this.runner = runner;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Writable newInstance() {
    Writable newInstance = (Writable) ReflectionUtils.newInstance(
        LOOKUP_LIST.get(writableClassIndex), conf);
    ((Vertex) newInstance).runner = this.runner;
    return newInstance;
  }
}
