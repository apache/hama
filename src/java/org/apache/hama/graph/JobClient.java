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
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.HamaMaster;
import org.apache.hama.ipc.JobSubmissionProtocol;

public class JobClient extends Configured {
  private static final Log LOG = LogFactory.getLog(JobClient.class);

  public static enum TaskStatusFilter {
    NONE, KILLED, FAILED, SUCCEEDED, ALL
  }

  static {
    Configuration.addDefaultResource("groomserver-default.xml");
  }

  private JobSubmissionProtocol jobSubmitClient;

  public JobClient() {
  }

  public JobClient(HamaConfiguration conf) throws IOException {
    setConf(conf);
    init(conf);
  }

  public void init(HamaConfiguration conf) throws IOException {
    String tracker = conf.get("hama.master.address", "local");
    this.jobSubmitClient = createRPCProxy(HamaMaster.getAddress(conf), conf);
  }

  private JobSubmissionProtocol createRPCProxy(InetSocketAddress addr,
      Configuration conf) throws IOException {
    return (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class,
        JobSubmissionProtocol.versionID, addr, conf, NetUtils.getSocketFactory(
            conf, JobSubmissionProtocol.class));
  }
}
