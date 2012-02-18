/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp.sync;

import org.apache.hadoop.conf.Configuration;

/**
 * Basic interface for a barrier synchronization services. This interface is not
 * used by HAMA, because every syncserver has to run as a seperate daemon. In
 * YARN this is used to launch the sync server as part of the ApplicationMaster
 * in a separate thread.
 * 
 */
public interface SyncServer {

  /**
   * In YARN port and hostname of the sync server is only known at runtime, so
   * this method should modify the conf to set the host:port of the syncserver
   * that is going to start and return it.<br/>
   * <br/>
   * The property key is "hama.sync.server.address" and the value is
   * hostname:port.
   * 
   * @param conf
   * @return
   */
  public Configuration init(Configuration conf) throws Exception;

  /**
   * Starts the server. This method can possibly block the call.
   */
  public void start() throws Exception;

  /**
   * Stops the server.
   */
  public void stopServer();

}
