/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.util;

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZKUtil {

  public static final Log LOG = LogFactory.getLog(ZKUtil.class);

  public static final String ZK_SEPARATOR = "/";

  /**
   * Recursively create ZooKeeper's znode with corresponded path, which starts
   * from the root (/).
   * 
   * @param zk is the target where znode is to be created.
   * @param path of the znode on ZooKeeper server.
   */
  public static void create(ZooKeeper zk, String path) {
    if (LOG.isDebugEnabled())
      LOG.debug("Path to be splitted: " + path);
    if (!path.startsWith(ZK_SEPARATOR))
      throw new IllegalArgumentException("Path is not started from root(/): "
          + path);
    StringTokenizer token = new StringTokenizer(path, ZKUtil.ZK_SEPARATOR);
    int count = token.countTokens();
    if (0 >= count)
      throw new RuntimeException("Can not correctly split the path into.");
    String[] parts = new String[count];
    int pos = 0;
    while (token.hasMoreTokens()) {
      parts[pos] = token.nextToken();
      if (LOG.isDebugEnabled())
        LOG.debug("Splitted string:" + parts[pos]);
      pos++;
    }
    StringBuilder builder = new StringBuilder();
    for (String part : parts) {
      try {
        builder.append(ZKUtil.ZK_SEPARATOR + part);
        if (null == zk.exists(builder.toString(), false)) {
          zk.create(builder.toString(), null, Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        }
      } catch (KeeperException ke) {
        LOG.warn(ke);
      } catch (InterruptedException ie) {
        LOG.warn(ie);
      }
    }
  }

}
