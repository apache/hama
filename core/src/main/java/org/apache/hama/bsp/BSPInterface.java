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
 * The {@link BSPInterface} defines the basic operations needed to implement a BSP
 * based algorithm.
 * The implementing algorithm takes {@link BSPPeer}s as parameters which are
 * responsible for communication, reading K1-V1 inputs, collecting k2-V2 outputs
 * and exchanging messages of type M.
 */
public interface BSPInterface<K1, V1, K2, V2, M extends Writable> {

    /**
     * This method is your computation method, the main work of your BSP should be
     * done here.
     *
     * @param peer Your BSPPeer instance.
     * @throws java.io.IOException
     * @throws org.apache.hama.bsp.sync.SyncException
     * @throws InterruptedException
     */
    public void bsp(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException, SyncException, InterruptedException;

    /**
     * This method is called before the BSP method. It can be used for setup
     * purposes.
     *
     * @param peer Your BSPPeer instance.
     * @throws IOException
     */
    public void setup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
            SyncException, InterruptedException;

    /**
     * This method is called after the BSP method. It can be used for cleanup
     * purposes. Cleanup is guranteed to be called after the BSP runs, even in
     * case of exceptions.
     *
     * @param peer Your BSPPeer instance.
     * @throws IOException
     */
    public void cleanup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException;
}
