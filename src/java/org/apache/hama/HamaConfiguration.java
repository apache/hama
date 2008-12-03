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

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Adds Hama configuration files to a Configuration
 */
public class HamaConfiguration extends HBaseConfiguration {
  /** constructor */
  public HamaConfiguration() {
    super();
    addHamaResources();
  }

  /**
   * Create a clone of passed configuration.
   * 
   * @param c Configuration to clone.
   */
  public HamaConfiguration(final Configuration c) {
    this();
    for (Entry<String, String> e : c) {
      set(e.getKey(), e.getValue());
    }
  }

  /**
   * Sets the number of map task
   * 
   * @param map
   */
  public void setNumMapTasks(int map) {
    set("mapred.map.tasks",String.valueOf(map));
  }

  /**
   * Sets the number of reduce task
   * 
   * @param reduce
   */
  public void setNumReduceTasks(int reduce) {
    set("mapred.reduce.tasks",String.valueOf(reduce));
  }
  
  /**
   * Gets the number of map task
   */
  public int getNumMapTasks() {
    return Integer.parseInt(get("mapred.map.tasks"));
  }

  /**
   * Gets the number of reduce task
   */
  public int getNumReduceTasks() {
    return Integer.parseInt(get("mapred.reduce.tasks"));
  }
  
  /**
   * Adds Hama configuration files to a Configuration
   */
  private void addHamaResources() {
    addResource("hama-site.xml");
  }
}
