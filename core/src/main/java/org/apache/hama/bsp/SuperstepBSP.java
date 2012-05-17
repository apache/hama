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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.sync.SyncException;

public class SuperstepBSP<K1, V1, K2, V2, M extends Writable> extends
    BSP<K1, V1, K2, V2, M> {

  private static final Log LOG = LogFactory.getLog(SuperstepBSP.class);

  private Superstep<K1, V1, K2, V2, M>[] supersteps;
  private int startSuperstep;

  @SuppressWarnings("unchecked")
  @Override
  public void setup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException {
    // instantiate our superstep classes
    String classList = peer.getConfiguration().get("hama.supersteps.class");
    String[] classNames = classList.split(",");

    LOG.debug("Size of classes = " + classNames.length);

    supersteps = new Superstep[classNames.length];
    Superstep<K1, V1, K2, V2, M> newInstance;
    for (int i = 0; i < classNames.length; i++) {

      try {
        newInstance = (Superstep<K1, V1, K2, V2, M>) ReflectionUtils
            .newInstance(Class.forName(classNames[i]), peer.getConfiguration());
      } catch (ClassNotFoundException e) {
        LOG.error((new StringBuffer("Could not instantiate a Superstep class ")
            .append(classNames[i])).toString(), e);
        throw new IOException(e);
      }
      newInstance.setup(peer);
      supersteps[i] = newInstance;
    }
    startSuperstep = peer.getConfiguration().getInt("attempt.superstep", 0);
  }

  @Override
  public void bsp(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException,
      SyncException, InterruptedException {
    for (int index = startSuperstep; index < supersteps.length; index++) {
      Superstep<K1, V1, K2, V2, M> superstep = supersteps[index];
      superstep.compute(peer);
      if (superstep.haltComputation(peer)) {
        break;
      }
      peer.sync();
      startSuperstep = 0;
    }
  }

  @Override
  public void cleanup(BSPPeer<K1, V1, K2, V2, M> peer) throws IOException {
    for (Superstep<K1, V1, K2, V2, M> superstep : supersteps) {
      superstep.cleanup(peer);
    }
  }

}
