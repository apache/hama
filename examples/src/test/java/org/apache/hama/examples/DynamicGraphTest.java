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
package org.apache.hama.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.junit.Test;

/**
 * Unit test for {@link org.apache.hama.examples.DynamicGraph}
 */
public class DynamicGraphTest extends TestCase {
  private static String OUTPUT = "/tmp/page-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void verifyResult() throws IOException {
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(fts.getPath())));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] split = line.split("\t");
        assertTrue(split[0].equals("sum"));
        assertTrue(split[1].equals("11"));
        System.out.println(split[0] + " : " + split[1]);
      }
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  @Test
  public void test() throws IOException, InterruptedException,
      ClassNotFoundException {
    try {
      DynamicGraph.main(new String[] { "src/test/resources/dg.txt", OUTPUT });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

}
