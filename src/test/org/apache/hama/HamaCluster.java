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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.bsp.LocalBSPCluster;

/**
 * Forming up the miniDfs and miniHbase
 */
public abstract class HamaCluster extends HBaseClusterTestCase {
  public static final Log LOG = LogFactory.getLog(HamaCluster.class);
  protected final static HamaConfiguration conf = new HamaConfiguration();

  protected void setUp() throws Exception {
    super.setUp();
    
    String[] args = new String[0];
    StringUtils.startupShutdownMessage(LocalBSPCluster.class, args, LOG);
    HamaConfiguration conf = new HamaConfiguration();
    //LocalBSPCluster cluster = new LocalBSPCluster(conf);
    //cluster.startup();
  }

  protected static HamaConfiguration getConf() {
    return conf;
  }
  
  protected void setMiniBSPCluster() {
    // TODO Auto-generated method stub    
  }
}
