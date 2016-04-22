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

  public static final String GROOM_RPC_HOST = "bsp.groom.rpc.hostname";

  public static final String DEFAULT_GROOM_RPC_HOST = "0.0.0.0";

  public static final String GROOM_RPC_PORT = "bsp.groom.rpc.port";

  /** Default port region rpc server listens on. */
  public static final int DEFAULT_GROOM_RPC_PORT = 50000;

  // /////////////////////////////////////
  // Constants for BSP Package
  // /////////////////////////////////////
  /** default host address */
  public static final String PEER_HOST = "bsp.peer.hostname";
  /** default host address */
  public static final String DEFAULT_PEER_HOST = "0.0.0.0";

  public static final String PEER_PORT = "bsp.peer.port";
  /** Default port region server listens on. */
  public static final int DEFAULT_PEER_PORT = 61000;

  public static final String PEER_ID = "bsp.peer.id";

  /** Parameter name for what groom server implementation to use. */
  public static final String GROOM_SERVER_IMPL = "hama.groomserver.impl";

  /** Parameter name for interval at which bsp peer should ping groomserver */
  public static final String GROOM_PING_PERIOD = "bsp.groomserver.pingperiod";

  /** Default value of ping period in milliseconds. */
  public static final long DEFAULT_GROOM_PING_PERIOD = 5000;

  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";

  public static final String MAX_TASKS_PER_GROOM = "bsp.tasks.maximum";

  public static final String MAX_TASKS = "bsp.tasks.maximum.total";

  public static final String MAX_TASK_ATTEMPTS = "bsp.tasks.max.attempts";

  public static final String MAX_TASKS_PER_JOB = "bsp.max.tasks.per.job";

  public static final String COMBINER_CLASS = "bsp.combiner.class";

  public static final int DEFAULT_MAX_TASK_ATTEMPTS = 2;

  // //////////////////////////////////////
  // Task scheduler related constants
  // //////////////////////////////////////

  public static final String TASK_ALLOCATOR_CLASS = "bsp.taskalloc.class";

  // //////////////////////////////////////
  // Fault tolerance related constants
  // //////////////////////////////////////

  public static final String FAULT_TOLERANCE_FLAG = "bsp.ft.enabled";

  public static final String FAULT_TOLERANCE_CLASS = "bsp.ft.class";

  // //////////////////////////////////////
  // Checkpointing related constants
  // //////////////////////////////////////

  // Set to true to enable checkpointing.
  public static final String CHECKPOINT_ENABLED = "bsp.checkpoint.enabled";
  // Superstep interval at which BSPPeer should initiate a checkpoint.
  public static final String CHECKPOINT_INTERVAL = "bsp.checkpoint.interval";
  // By default checkpointing when enabled would checkpoint on every superstep
  public static final short DEFAULT_CHECKPOINT_INTERVAL = 1;

  // /////////////////////////////////////////////
  // Executor related parameters.
  // /////////////////////////////////////////////
  public static final String TASK_EXECUTOR_CLASS = "bsp.master.TaskWorkerManager.class";
  
  // /////////////////////////////////////////////
  // Job configuration related parameters.
  // /////////////////////////////////////////////
  public static final String JOB_INPUT_DIR = "bsp.input.dir";
  public static final String JOB_OUTPUT_DIR = "bsp.output.dir";
  public static final String JOB_PEERS_COUNT = "bsp.peers.num";
  public static final String INPUT_FORMAT_CLASS = "bsp.input.format.class";
  public static final String OUTPUT_FORMAT_CLASS = "bsp.output.format.class";
  public static final String MESSAGE_CLASS = "bsp.message.class";

  // /////////////////////////////////////////////
  // Messaging related parameters.
  // /////////////////////////////////////////////
  public static final int BUFFER_DEFAULT_SIZE = 16 * 1024;
  public static final String BYTEBUFFER_SIZE = "bsp.message.bytebuffer.size";
  public static final String BYTEBUFFER_DIRECT = "bsp.message.bytebuffer.direct";
  public static final boolean BYTEBUFFER_DIRECT_DEFAULT = true;
  public static final String DATA_SPILL_PATH = "bsp.data.spill.location";

  // /////////////////////////////////////////////
  // Constants related to partitioning
  // /////////////////////////////////////////////
  public static final String RUNTIME_PARTITIONING_DIR = "bsp.partitioning.dir";
  public static final String ENABLE_RUNTIME_PARTITIONING = "bsp.input.runtime.partitioning";
  public static final String RUNTIME_PARTITIONING_CLASS = "bsp.input.partitioner.class";
  public static final String RUNTIME_DESIRED_PEERS_COUNT = "desired.num.of.tasks";
  public static final String RUNTIME_PARTITION_RECORDCONVERTER = "bsp.runtime.partition.recordconverter";
  public static final String PARTITION_SORT_BY_KEY = "bsp.partition.sort.by.converted.record";

  // If true, framework launches the number of tasks by user settings.
  public static final String FORCE_SET_BSP_TASKS = "hama.force.set.bsp.tasks";
  
  // framework launches additional tasks to the number of input splits
  public static final String ADDITIONAL_BSP_TASKS = "hama.additional.bsp.tasks";
  
  // /////////////////////////////////////
  // Constants for ZooKeeper
  // /////////////////////////////////////
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
  static final String MESSENGER_RUNTIME_COMPRESSION = "hama.messenger.runtime.compression";
  
  /**
   * An empty instance.
   */
  static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  public static final int DEFAULT_GROOM_INFO_SERVER = 40015;
}
