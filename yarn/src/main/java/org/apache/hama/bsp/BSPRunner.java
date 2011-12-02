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

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.BSPPeerProtocol;
import org.apache.hama.util.BSPNetUtils;

public class BSPRunner {

  private static final Log LOG = LogFactory.getLog(BSPRunner.class);

  private Configuration conf;
  private TaskAttemptID id;
  private BSPPeerImpl<?, ?, ?, ?> peer;
  private Counters counters = new Counters();
  
  @SuppressWarnings("rawtypes")
  Class<? extends BSP> bspClass;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public BSPRunner(String jobId, int taskAttemptId, Path confPath)
      throws Exception {
    conf = new HamaConfiguration();
    conf.addResource(confPath);
    this.id = new TaskAttemptID(jobId, 0, taskAttemptId, 0);
    this.id.id = taskAttemptId;

    // use a calculatory trick to prevent port collision on the same host
    int port = BSPNetUtils.getFreePort(taskAttemptId * 2 + 16000);
    conf.setInt(Constants.PEER_PORT, port);
    conf.set(Constants.PEER_HOST, BSPNetUtils.getCanonicalHostname());

    String umbilicalAddress = conf.get("hama.umbilical.address");
    if (umbilicalAddress == null || umbilicalAddress.isEmpty()
        || !umbilicalAddress.contains(":")) {
      throw new IllegalArgumentException(
          "Umbilical address must contain a colon and must be non-empty and not-null! Property \"hama.umbilical.address\" was: "
              + umbilicalAddress);
    }
    String[] hostPort = umbilicalAddress.split(":");
    InetSocketAddress address = new InetSocketAddress(hostPort[0],
        Integer.valueOf(hostPort[1]));

    BSPPeerProtocol umbilical = (BSPPeerProtocol) RPC.getProxy(
        BSPPeerProtocol.class, BSPPeerProtocol.versionID, address, conf);

    BSPJob job = new BSPJob(new HamaConfiguration(conf));

    peer = new BSPPeerImpl(job, conf, id, umbilical, port, umbilicalAddress,
        null, counters);
    // this is a checked cast because we can only set a class via the BSPJob
    // class which only allows derivates of BSP.
    bspClass = (Class<? extends BSP>) conf.getClassByName(conf
        .get("bsp.work.class"));
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void startComputation() throws Exception {
    BSP bspInstance = ReflectionUtils.newInstance(bspClass, conf);
    try {
      bspInstance.setup(peer);
      bspInstance.bsp(peer);
    } catch (Exception e) {
      throw e;
    } finally {
      bspInstance.cleanup(peer);
      peer.close();
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
    LOG.info("Task successfully ended!");
  }
}
