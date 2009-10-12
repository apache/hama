/**
 * Copyright 2007 The Apache Software Foundation
 *
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

public interface BSPConstants {

  /** default host address */
  static final String PEER_HOST = "bsp.peer.hostname";
  /** default host address */
  static final String DEFAULT_PEER_HOST = "0.0.0.0";

  static final String PEER_PORT = "bsp.peer.port";
  /** Default port region server listens on. */
  static final int DEFAULT_PEER_PORT = 61000;

  static final long ATLEAST_WAIT_TIME = 100;

  /** zookeeper root */
  static final String ZOOKEEPER_ROOT = "bsp.zookeeper.root";
  /** zookeeper default root */
  static final String DEFAULT_ZOOKEEPER_ROOT = "/bsp";

  /** zookeeper server address */
  static final String ZOOKEEPER_SERVER_ADDRS = "zookeeper.server";
  /** Parameter name for number of times to retry writes to ZooKeeper. */
  static final String ZOOKEEPER_RETRIES = "zookeeper.retries";
  /** Default number of times to retry writes to ZooKeeper. */
  static final int DEFAULT_ZOOKEEPER_RETRIES = 5;
  /** Parameter name for ZooKeeper pause between retries. In milliseconds. */
  static final String ZOOKEEPER_PAUSE = "zookeeper.pause";
  /** Default ZooKeeper pause value. In milliseconds. */
  static final int DEFAULT_ZOOKEEPER_PAUSE = 2 * 1000;

  /**
   * An empty instance.
   */
  static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
}
