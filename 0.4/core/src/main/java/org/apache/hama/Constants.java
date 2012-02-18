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
package org.apache.hama;

/**
 * Some constants used in the Hama.
 */
public interface Constants {
 
  public  static final String GROOM_RPC_HOST = "bsp.groom.rpc.hostname";

  public static final String DEFAULT_GROOM_RPC_HOST = "0.0.0.0";

  public static final String GROOM_RPC_PORT = "bsp.groom.rpc.port";

  /** Default port region rpc server listens on. */
  public static final int DEFAULT_GROOM_RPC_PORT = 50000;
  

  ///////////////////////////////////////
  // Constants for BSP Package
  ///////////////////////////////////////
  /** default host address */
  public  static final String PEER_HOST = "bsp.peer.hostname";
  /** default host address */
  public static final String DEFAULT_PEER_HOST = "0.0.0.0";

  public static final String PEER_PORT = "bsp.peer.port";
  /** Default port region server listens on. */
  public static final int DEFAULT_PEER_PORT = 61000;

  public static final String PEER_ID = "bsp.peer.id";
  
  /** Parameter name for what groom server implementation to use. */
  public static final String GROOM_SERVER_IMPL= "hama.groomserver.impl";
  
  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";
  
  public static final String MAX_TASKS_PER_GROOM = "bsp.tasks.maximum";
  
  ///////////////////////////////////////
  // Constants for ZooKeeper
  ///////////////////////////////////////  
  /** zookeeper root */
  public static final String ZOOKEEPER_ROOT = "bsp.zookeeper.root";
  /** zookeeper default root */
  public static final String DEFAULT_ZOOKEEPER_ROOT = "/bsp";

  /** zookeeper server address */
  public static final String ZOOKEEPER_SERVER_ADDRS = "zookeeper.server";
  /** zookeeper default server address */
  static final String DEFAULT_ZOOKEEPER_SERVER_ADDR = "localhost:21810";
  /** Parameter name for number of times to retry writes to ZooKeeper. */
  public static final String ZOOKEEPER_RETRIES = "zookeeper.retries";
  /** Default number of times to retry writes to ZooKeeper. */
  public static final int DEFAULT_ZOOKEEPER_RETRIES = 5;
  /** Parameter name for ZooKeeper pause between retries. In milliseconds. */
  public static final String ZOOKEEPER_PAUSE = "zookeeper.pause";
  /** Default ZooKeeper pause value. In milliseconds. */
  public static final int DEFAULT_ZOOKEEPER_PAUSE = 2 * 1000;
  
  static final String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";
  static final String ZOOKEEPER_CLIENT_PORT = "hama.zookeeper.property.clientPort";
  static final String ZOOKEEPER_SESSION_TIMEOUT = "hama.zookeeper.session.timeout";
  static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 21810;
  static final String ZOOKEEPER_QUORUM = "hama.zookeeper.quorum";
  /** Cluster is in distributed mode or not */
  static final String CLUSTER_DISTRIBUTED = "hama.cluster.distributed";
  /** Cluster is fully-distributed */
  static final String CLUSTER_IS_DISTRIBUTED = "true";


  // Other constants

  /**
   * An empty instance.
   */
  static final byte [] EMPTY_BYTE_ARRAY = new byte [0];
}
