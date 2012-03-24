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

public abstract class Superstep<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE extends Writable> {

  /**
   * Setup this superstep, is called once before compute().
   */
  protected void setup(BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer) {

  }

  /**
   * Cleanup this superstep, is called once after compute().
   */
  protected void cleanup(BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer) {

  }

  /**
   * Main computation phase.
   */
  protected abstract void compute(
      BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer)
      throws IOException;

  /**
   * @return true to halt the computation
   */
  protected boolean haltComputation(
      BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer) {
    return false;
  }

}
