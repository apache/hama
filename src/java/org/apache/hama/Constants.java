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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/**
 * Some constants used in the Hama
 */
public interface Constants {

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

  public static final long ATLEAST_WAIT_TIME = 100;

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
  static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 21810;
  static final String ZOOKEEPER_QUORUM = "hama.zookeeper.quorum";
  /** Cluster is in distributed mode or not */
  static final String CLUSTER_DISTRIBUTED = "hama.cluster.distributed";
  /** Cluster is fully-distributed */
  static final String CLUSTER_IS_DISTRIBUTED = "true";
  ///////////////////////////////////////
  // Constants for Matrix Package
  ///////////////////////////////////////
  /**
   * An empty instance.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  
  /**
   * Hbase Structure for matrices 
   */
  
  /** Meta-columnFamily to store the matrix-info */
  public final static String METADATA = "metadata";

  /** Column index & attributes */
  public final static String CINDEX = "cIndex";

  /** The attribute column family */
  public static byte[] ATTRIBUTE = Bytes.toBytes("attribute");

  /** The type of the matrix */
  public final static String METADATA_TYPE = "type";
  
  /** The reference of the matrix */
  /** (1) when we create a Matrix object, we set up a connection to hbase table,
   *      the reference of the table will be incremented.
   *  (2) when we close a Matrix object, we disconnect the hbase table, 
   *      the reference of the table will be decremented.
   *      i)  if the reference of the table is not zero:
   *          we should not delete the table, because some other matrix object
   *          connect to the table.
   *      ii) if the reference of the table is zero:
   *          we need to know if the matrix table is aliased.
   *          1) if the matrix table is aliased, we should not delete the table.
   *          2) if the matrix table is not aliased, we need to delete the table.
   */
  public final static String METADATA_REFERENCE = "reference";
  
  /** The aliase names column family */
  public final static String ALIASEFAMILY = "aliase";
  
  /** Default columnFamily name */
  public static byte[] COLUMNFAMILY = Bytes.toBytes("column");
  
  /** Temporary random matrices name prefix */
  public final static String RANDOM = "rand";

  /** Admin table name */
  public final static String ADMINTABLE = "hama.admin.table";

  /** Matrix path columnFamily */
  public static final String PATHCOLUMN = "path";

  /** Temporary Aliase name prefix in Hama Shell */
  public static final String RANDOMALIASE = "_";
  
  /** default matrix's path length (tablename length) */
  public static final int DEFAULT_PATH_LENGTH = 5;
  
  /** default matrix's max path length (tablename length) */
  public static final int DEFAULT_MAXPATHLEN = 10000;
  
  /** default try times to generate a suitable tablename */
  public static final int DEFAULT_TRY_TIMES = 10000000;
  
  /** block data column */
  public static final String BLOCK = "block";
  
  public static final Text ROWCOUNT= new Text("row");
  public static final String PEER_ID = "bsp.peer.id";
}
