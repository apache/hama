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
package org.apache.hama.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.TestCase;
import static org.junit.Assert.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class TestZKUtil extends TestCase {

  MockZK zk;
  String path;
  String[] parts;
  int pos = 0;
  StringBuffer sb = new StringBuffer();

  class MockZK extends ZooKeeper {

    public MockZK(String connectString, int timeout, Watcher watcher) 
        throws IOException { 
      super(connectString, timeout, watcher);
    }
   
    // create is called in for loop 
    public String create(String path, byte[] data, List<ACL> acl, 
        CreateMode createMode) throws KeeperException, InterruptedException {  
      parts[pos] = path; 
      pos++;
      sb.append(File.separator+path);
      StringBuilder builder = new StringBuilder();
      for(int i=0;i<pos;i++) {
        builder.append(File.separator+parts[i]);
      }
      assertEquals("Make sure path created is consistent.", sb.toString(), builder.toString());
      return path;
    }
  }

  public void setUp() throws Exception {
    this.zk = new MockZK("localhost:2181", 3000, null);
    this.path = "/monitor/groom_lab01_61000/metrics/jvm";
    StringTokenizer token = new StringTokenizer(path, File.separator);
    int count = token.countTokens(); // should be 4
    assertEquals("Make sure token are 4.", count, 4);
    this.parts = new String[count]; // 
  }

  public void testCreatePath() throws Exception {
    ZKUtil.create(this.zk, path); 
  }

}
