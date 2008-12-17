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
 * Some constants used in the Hama
 */
public class Constants {

  /** Meta-columnFamily to store the matrix-info */
  public final static String METADATA = "metadata";

  /** Column index & attributes */
  public final static String CINDEX = "cIndex";

  /** The attribute column family */
  public final static String ATTRIBUTE = "attribute:";

  /** The number of the matrix rows */
  public final static String METADATA_ROWS = "attribute:rows";

  /** The number of the matrix columns */
  public final static String METADATA_COLUMNS = "attribute:columns";

  /** The type of the matrix */
  public final static String METADATA_TYPE = "attribute:type";
  
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
  public final static String METADATA_REFERENCE = "attribute:reference";
  
  /** The aliase names column family */
  public final static String ALIASEFAMILY = "aliase:";
  
  /** The aliase names of the matrix, sperated by "," */
  public final static String ALIASENAME = "aliase:name";

  /** Default columnFamily name */
  public final static String COLUMN = "column:";

  /** Temporary random matrices name prefix */
  public final static String RANDOM = "rand";

  /** Admin table name */
  public final static String ADMINTABLE = "admin.table";

  /** Matrix path columnFamily */
  public static final String PATHCOLUMN = "path:";

  /** Temporary Aliase name prefix in Hama Shell */
  public static final String RANDOMALIASE = "_";
  
  /** default matrix's path length (tablename length) */
  public static final int DEFAULT_PATH_LENGTH = 5;
  
  /** default matrix's max path length (tablename length) */
  public static final int DEFAULT_MAXPATHLEN = 10000;
  
  /** default try times to generate a suitable tablename */
  public static final int DEFAULT_TRY_TIMES = 10000000;
  
  /**
   * block position column to store 
   * {@link org.apache.hama.io.BlockPosition} object
   */
  public static final String BLOCK_POSITION = "attribute:blockPosition";
  
  /** block data column */
  public static final String BLOCK = "block:";

  /** block size */
  public static final String BLOCK_SIZE = "attribute:blockSize";
}
