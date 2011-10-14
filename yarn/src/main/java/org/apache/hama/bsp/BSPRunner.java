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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.HamaConfiguration;

public class BSPRunner {

  private static final Log LOG = LogFactory.getLog(BSPRunner.class);

  private Configuration conf;
  private TaskAttemptID id;
  private YARNBSPPeerImpl peer;

  Class<? extends BSP> bspClass;

  @SuppressWarnings("unchecked")
  public BSPRunner(String jobId, int taskAttemptId, Path confPath)
      throws IOException, ClassNotFoundException {
    conf = new HamaConfiguration();
    conf.addResource(confPath);
    this.id = new TaskAttemptID(jobId, 0, taskAttemptId, 0);
    this.id.id = taskAttemptId;
    peer = new YARNBSPPeerImpl(conf, id);
    // this is a checked cast because we can only set a class via the BSPJob
    // class which only allows derivates of BSP.
    bspClass = (Class<? extends BSP>) conf.getClassByName(conf
        .get("bsp.work.class"));
  }

  public void startComputation() throws Exception {
    BSP bspInstance = ReflectionUtils.newInstance(bspClass, conf);
    LOG.info("Syncing for the first time to wait for all the tasks to come up...");
    peer.getSyncService().enterBarrier(id);
    peer.getSyncService().leaveBarrier(id);
    LOG.info("Initial sync was successful, now running the computation!");
    try {
      bspInstance.setup(peer);
      bspInstance.bsp(peer);
    } catch (Exception e) {
      throw e;
    } finally {
      bspInstance.cleanup(peer);
    }
  }

  /**
   * Main entry point after a container has launched.
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Starting task with arguments: " + Arrays.toString(args));
    if (args.length != 3) {
      throw new IllegalArgumentException("Expected 3 args given, but found: "
          + Arrays.toString(args));
    }
    // jobid is the first of the args (string)
    // taskid is the second arg (int)
    // third arg is the qualified path of the job configuration
    BSPRunner bspRunner = new BSPRunner(args[0], Integer.valueOf(args[1]),
        new Path(args[2]));
    bspRunner.startComputation();
  }
}
