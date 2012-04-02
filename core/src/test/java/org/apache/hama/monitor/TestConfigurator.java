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
package org.apache.hama.monitor;

import java.util.Map;

import junit.framework.TestCase;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hama.HamaConfiguration;
import org.apache.hama.monitor.Monitor.Task;

public class TestConfigurator extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestConfigurator.class);

  /**
   * If test fails, please check if `plugins' dir exists under "user.dir".
   */
  public void testPluginDirNotPresented() throws Exception {
    System.setProperty("hama.home.dir", System.getProperty("user.dir"));
    Map<String, Task> tasks = Configurator.configure(new HamaConfiguration(), null);    
    LOG.info("Plugins dir is not created, returned tasks should be null -> "+tasks);
    assertNull("Tasks returned should be null because no plugins dir is created.", tasks);
  }

  
}
