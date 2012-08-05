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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.taskallocation.BSPResource;
import org.apache.hama.bsp.taskallocation.BestEffortDataLocalTaskAllocator;
import org.apache.hama.bsp.taskallocation.TaskAllocationStrategy;

public class TestTaskAllocation extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestTaskAllocation.class);

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testBestEffortDataLocality() throws Exception {

    Configuration conf = new Configuration();

    String[] locations = new String[] { "host6", "host4", "host3" };
    String value = "data";
    RawSplit split = new RawSplit();
    split.setLocations(locations);
    split.setBytes(value.getBytes(), 0, value.getBytes().length);
    split.setDataLength(value.getBytes().length);

    assertEquals(value.getBytes().length, (int) split.getDataLength());

    Map<GroomServerStatus, Integer> taskCountInGroomMap = new HashMap<GroomServerStatus, Integer>(
        20);
    BSPResource[] resources = new BSPResource[0];
    BSPJob job = new BSPJob(new BSPJobID("checkpttest", 1), "/tmp");
    JobInProgress jobProgress = new JobInProgress(job.getJobID(), conf);
    TaskInProgress taskInProgress = new TaskInProgress(job.getJobID(),
        "job.xml", split, conf, jobProgress, 1);

    Map<String, GroomServerStatus> groomStatuses = new HashMap<String, GroomServerStatus>(
        20);

    for (int i = 0; i < 10; ++i) {

      String name = "host" + i;
      GroomServerStatus status = new GroomServerStatus(name,
          new ArrayList<TaskStatus>(), 0, 3);
      groomStatuses.put(name, status);
      taskCountInGroomMap.put(status, 0);

    }

    TaskAllocationStrategy strategy = ReflectionUtils.newInstance(conf
        .getClass("", BestEffortDataLocalTaskAllocator.class,
            TaskAllocationStrategy.class), conf);

    String[] hosts = strategy.selectGrooms(groomStatuses, taskCountInGroomMap,
        resources, taskInProgress);

    List<String> list = new ArrayList<String>();

    for (int i = 0; i < hosts.length; ++i) {
      list.add(hosts[i]);
    }

    assertTrue(list.contains("host6"));
    assertTrue(list.contains("host3"));
    assertTrue(list.contains("host4"));

  }

}
