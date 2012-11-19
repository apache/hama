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

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.sync.SyncException;

/**
 * This class provides an abstract implementation of the {@link BSPInterface}.
 */
public abstract class BSP<K1, V1, K2, V2, M extends Writable> implements
    BSPInterface<K1, V1, K2, V2, M> {

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void bsp(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException {

  }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException {

  }

}
