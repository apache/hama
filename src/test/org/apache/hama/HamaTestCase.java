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

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Forming up the miniDfs and miniHbase
 */
public class HamaTestCase extends HBaseClusterTestCase {
  static final Logger LOG = Logger.getLogger(HamaTestCase.class);
  protected Matrix matrixA;
  protected Matrix matrixB;
  protected Text A = new Text("matrixA");
  protected Text B = new Text("matrixB");
  protected int SIZE = 5;
  
  /** constructor */
  public HamaTestCase() {
    super();

    // Initializing the hbase configuration
    conf.set("mapred.output.dir", conf.get("hadoop.tmp.dir"));

    conf.setInt("mapred.map.tasks", 10);
    conf.setInt("mapred.reduce.tasks", 1);

    conf.setInt("hbase.hregion.memcache.flush.size", 1024 * 1024);
    conf.setInt("hbase.hstore.compactionThreshold", 2);
    conf.setLong("hbase.hregion.max.filesize", 1024 * 1024);
    conf.setInt("hbase.master.lease.period", 10 * 1000);
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);
    conf.setInt("hbase.client.pause", 10 * 1000);
  }
}
