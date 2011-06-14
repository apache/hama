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
package org.apache.hama.zookeeper;

import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;

import junit.framework.TestCase;

public class TestZKTools extends TestCase {

  public void testZKProps() {
    HamaConfiguration conf = new HamaConfiguration();
    conf.set(Constants.ZOOKEEPER_QUORUM, "test.com:123");
    conf.set(Constants.ZOOKEPER_CLIENT_PORT, "2222");

    assertEquals("test.com:2222", QuorumPeer.getZKQuorumServersString(conf));
  }
}
