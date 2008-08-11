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

import org.apache.hadoop.io.Text;

/**
 * Some constants used in the hama
 */
public class Constants {
  /** Meta-columnfamily to store the matrix-info */
  public final static Text METADATA = new Text("metadata:");
  /** The number of the matrix rows */
  public final static Text METADATA_ROWS = new Text("metadata:rows");
  /** The number of the matrix columns */
  public final static Text METADATA_COLUMNS = new Text("metadata:columns");
  /** The type of the matrix */
  public final static Text METADATA_TYPE = new Text("metadata:type");
  /** Default columnfamily name */
  public final static String COLUMN = "column:";

  /** Temporary random matices name prefix */
  public final static String RANDOM = "rand";
}
