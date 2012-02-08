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

import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPMessage;

public class GraphJob extends BSPJob {
  public final static String VERTEX_CLASS_ATTR = "hama.graph.vertex.class";

  public GraphJob(HamaConfiguration conf) throws IOException {
    super(conf);
    this.setBspClass(GraphJobRunner.class);
  }

  /**
   * Set the Vertex class for the job.
   * 
   * @param cls
   * @throws IllegalStateException
   */
  public void setVertexClass(Class<? extends Vertex<? extends BSPMessage>> cls)
      throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(VERTEX_CLASS_ATTR, cls, Vertex.class);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Vertex<? extends BSPMessage>> getVertexClass() {
    return (Class<? extends Vertex<? extends BSPMessage>>) conf.getClass(
        VERTEX_CLASS_ATTR, Vertex.class);
  }

  // TODO this method should be moved into BSPJob
  public void setMaxIteration(int maxIteration) {
    conf.setInt("hama.graph.max.iteration", maxIteration);
  }
}
