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
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.MindistSearch.MinTextCombiner;

public class MindistSearchTest extends TestCase {

  String[] resultList = new String[] { "0", "1", "2", "2", "1", "2", "2", "1",
      "2", "0" };
  String[] input = new String[] { "0", "1\t4\t7", "2\t3\t8", "3\t5", "4\t1",
      "5\t6", "6", "7", "8\t3", "9\t0" };

  private static String INPUT = "/tmp/mdst-tmp.seq";
  private static String TEXT_INPUT = "/tmp/mdst.txt";
  private static String TEXT_OUTPUT = INPUT + "mdst.txt.seq";
  private static String OUTPUT = "/tmp/mdst-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  public void testMindistSearch() throws Exception {
    generateTestData();
    try {
      MindistSearch.main(new String[] { INPUT, OUTPUT, "30", "2" });

      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  public void testMinTextCombiner() throws Exception {
    MinTextCombiner combiner = new MinTextCombiner();
    Text a = new Text("1");
    Text b = new Text("2");
    Text d = new Text("4");
    Text c = new Text("3");
    List<Text> asList = Arrays.asList(new Text[] { a, b, c, d });
    Text combine = combiner.combine(asList);
    assertEquals(combine, a);
  }

  private void verifyResult() throws IOException {
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    int itemsRead = 0;
    for (FileStatus fts : globStatus) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(fts.getPath())));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] split = line.split("\t");
        System.out.println(split[0] + " | " + split[1]);
        assertEquals(resultList[Integer.parseInt(split[0])], split[1]);
        itemsRead++;
      }
    }
    assertEquals(resultList.length, itemsRead);
  }

  private void generateTestData() {
    BufferedWriter bw = null;
    try {
      bw = new BufferedWriter(new FileWriter(INPUT));
      for (String s : input) {
        bw.write(s + "\n");
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
