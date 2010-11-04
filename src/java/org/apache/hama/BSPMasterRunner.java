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
package org.apache.hama;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hama.bsp.BSPMaster;

/**
 * This class starts and runs the BSPMaster
 */
public class BSPMasterRunner extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(BSPMasterRunner.class);

  @Override
  public int run(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(BSPMaster.class, args, LOG);

    if (args.length != 0) {
      System.out.println("usage: BSPMasterRunner");
      System.exit(-1);
    }

    try {
      HamaConfiguration conf = new HamaConfiguration(getConf());
      BSPMaster master = BSPMaster.startMaster(conf);
      master.offerService();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new BSPMasterRunner(), args);
    System.exit(exitCode);
  }

}
