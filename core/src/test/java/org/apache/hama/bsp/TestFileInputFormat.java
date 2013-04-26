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
package org.apache.hama.bsp;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;

public class TestFileInputFormat extends TestCase {

  public void testComputeGoalSize() throws Exception {

    TextInputFormat input = new TextInputFormat();
    assertTrue(1000 < input.computeGoalSize(10, 10000)
        && 1200 > input.computeGoalSize(10, 10000));

  }

  public void testSetInputPaths() throws IOException {
    HamaConfiguration conf = new HamaConfiguration();
    BSPJob job = new BSPJob(conf);

    String[] files = new String[2];
    files[0] = "hdfs://hadoop.uta.edu/user/hadoop/employee.txt";
    files[1] = "hdfs://hadoop.uta.edu/user/hadoop/department.txt";

    FileInputFormat.setInputPaths(job, files[0] + "," + files[1]);
    Path[] paths = FileInputFormat.getInputPaths(job);

    System.out.println(job.getConfiguration().get("bsp.input.dir"));
    assertEquals(2, FileInputFormat.getInputPaths(job).length);

    for (int i = 0; i < paths.length; i++) {
      System.out.println(paths[i]);
      assertEquals(paths[i].toString(), files[i]);
    }
  }
}
