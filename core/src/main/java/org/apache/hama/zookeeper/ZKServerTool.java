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
package org.apache.hama.zookeeper;

import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;

/**
 * A tool class for Zookeeper use.
 */
public class ZKServerTool {
  
  /**
   * Run the tool.
   * 
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) {
    Configuration conf = new HamaConfiguration();
    // Note that we do not simply grab the property ZOOKEEPER_QUORUM from
    // the HamaConfiguration because the user may be using a zoo.cfg file.
    Properties zkProps = QuorumPeer.makeZKProps(conf);
    for (Entry<Object, Object> entry : zkProps.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        String[] parts = value.split(":");
        String host = parts[0];
        System.out.println(host);
      }
    }
  }
}
