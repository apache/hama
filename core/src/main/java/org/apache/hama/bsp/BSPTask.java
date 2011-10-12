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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.zookeeper.KeeperException;

/**
 * Base class for tasks.
 */
public class BSPTask extends Task {

  public static final Log LOG = LogFactory.getLog(BSPTask.class);

  private BSPJob conf;

  public BSPTask() {
  }

  public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid,
      int partition) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.taskId = taskid;
    this.partition = partition;
  }

  @Override
  public BSPTaskRunner createRunner(GroomServer groom) {
    return new BSPTaskRunner(this, groom, this.conf);
  }

  @Override
  public void run(BSPJob job, BSPPeer bspPeer, BSPPeerProtocol umbilical)
      throws IOException {

    BSP bsp = (BSP) ReflectionUtils.newInstance(
        job.getConf().getClass("bsp.work.class", BSP.class), job.getConf());

    bsp.setup(bspPeer);

    try {
      bsp.bsp(bspPeer);
    } catch (IOException e) {
      LOG.error("Exception during BSP execution!", e);
    } catch (KeeperException e) {
      LOG.error("Exception during BSP execution!", e);
    } catch (InterruptedException e) {
      LOG.error("Exception during BSP execution!", e);
    } finally {
      bsp.cleanup(bspPeer);
    }

    done(umbilical);
  }

  public BSPJob getConf() {
    return conf;
  }

  public void setConf(BSPJob conf) {
    this.conf = conf;
  }

}
