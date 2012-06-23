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
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;

/**
 * Testcase for {@link ShortestPaths}
 */
public class SSSPTest extends TestCase {
  String[] input = new String[] { "1:85\t2:217\t4:173", "0:85\t5:80",
      "0:217\t6:186\t7:103", "7:183", "0:173\t9:502", "1:80\t8:250", "2:186",
      "3:183\t9:167\t2:103", "5:250\t9:84", "4:502\t7:167\t8:84" };

  private static String INPUT = "/tmp/sssp-tmp.seq";
  private static String TEXT_INPUT = "/tmp/sssp.txt";
  private static String TEXT_OUTPUT = INPUT + "sssp.txt.seq";
  private static String OUTPUT = "/tmp/sssp-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  public void testShortestPaths() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    generateTestData();
    try {
      SSSP.main(new String[] { "0", INPUT, OUTPUT, "2" });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    Map<String, Integer> rs = new HashMap<String, Integer>();
    rs.put("6", 403);
    rs.put("1", 85);
    rs.put("3", 503);
    rs.put("4", 173);
    rs.put("7", 320);
    rs.put("8", 415);
    rs.put("0", 0);
    rs.put("9", 487);
    rs.put("2", 217);
    rs.put("5", 165);

    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(fts.getPath())));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] split = line.split("\t");
        assertEquals(Integer.parseInt(split[1]), (int) rs.get(split[0]));
      }
    }
  }

  private void generateTestData() {
    BufferedWriter bw = null;
    try {
      bw = new BufferedWriter(new FileWriter(INPUT));
      int index = 0;
      for (String s : input) {
        bw.write((index++) + "\t" + s + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
      if (fs.exists(new Path(TEXT_INPUT)))
        fs.delete(new Path(TEXT_INPUT), true);
      if (fs.exists(new Path(TEXT_OUTPUT)))
        fs.delete(new Path(TEXT_OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
