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
package org.apache.hama.bsp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class TestClusterStatus extends TestCase {
  Random rnd = new Random();

  protected void setUp() throws Exception {
    super.setUp();
  }

  public final void testWriteAndReadFields() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    ClusterStatus status1;
    Map<String, GroomServerStatus> grooms = new HashMap<String, GroomServerStatus>();

    for (int i = 0; i < 10; i++) {
      int num = rnd.nextInt();
      String groomName = "groom_" + num;
      String peerName = "peerhost:" + num;
      grooms.put(groomName, new GroomServerStatus(peerName, new ArrayList<TaskStatus>(0), 25, 2));
    }

    int tasks = rnd.nextInt(100);
    int maxTasks = rnd.nextInt(100);
    BSPMaster.State state = BSPMaster.State.RUNNING;

    status1 = new ClusterStatus(grooms, tasks, maxTasks, state);
    status1.write(out);

    in.reset(out.getData(), out.getLength());

    ClusterStatus status2 = new ClusterStatus();
    status2.readFields(in);
    
    for(Entry<String, GroomServerStatus> entry : status2.getActiveGroomServerStatus().entrySet()){
      assertEquals(entry.getValue().getMaxTasks(),2);
      assertEquals(entry.getValue().getFailures(),25);
    }

    Map<String, String> grooms_s = new HashMap<String, String>(
        status1.getActiveGroomNames());
    Map<String, String> grooms_o = new HashMap<String, String>(
        status2.getActiveGroomNames());

    assertEquals(status1.getGroomServers(), status2.getGroomServers());

    assertTrue(grooms_s.entrySet().containsAll(grooms_o.entrySet()));
    assertTrue(grooms_o.entrySet().containsAll(grooms_s.entrySet()));

    assertEquals(status1.getTasks(), status2.getTasks());
    assertEquals(status1.getMaxTasks(), status2.getMaxTasks());
  }
}
