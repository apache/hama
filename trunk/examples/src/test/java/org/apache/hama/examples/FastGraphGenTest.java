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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.examples.util.FastGraphGen;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class FastGraphGenTest extends TestCase {
  protected static Log LOG = LogFactory.getLog(FastGraphGenTest.class);
  private static String TEST_OUTPUT = "/tmp/test";

  @Test
  public void testGraphGenerator() throws Exception {
    Configuration conf = new Configuration();

    // vertex size : 20
    // maximum edges : 10
    // output path : /tmp/test
    // tasks num : 3
    FastGraphGen.main(new String[] { "-v", "20", "-e", "10",
        "-o", TEST_OUTPUT, "-t", "3" });
    FileSystem fs = FileSystem.get(conf);

    FileStatus[] globStatus = fs.globStatus(new Path(TEST_OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      BufferedReader br = new BufferedReader(
          new InputStreamReader(fs.open(fts.getPath())));
      try {
        String line;
        line = br.readLine();
        while (line != null) {
          String[] keyValue = line.split("\t");
          String[] outlinkId = keyValue[1].split(" ");
          assertTrue(outlinkId.length <= 10);
          for (String edge : outlinkId) {
            assertTrue(Integer.parseInt(edge) < 20);
            assertTrue(Integer.parseInt(edge) >= 0);
          }
          line = br.readLine();
        }
      } finally {
        br.close();
      }
    }

    fs.delete(new Path(TEST_OUTPUT), true);
  }

  @Test
  public void testJsonGraphGenerator() throws Exception {
    Configuration conf = new Configuration();

    // vertex size : 20
    // maximum edges : 10
    // output path : /tmp/test
    // tasks num : 3
    // output type : json
    // weight : 0
    FastGraphGen.main(new String[] { "-v", "20", "-e", "10",
        "-o", TEST_OUTPUT, "-t", "1", "-of", "json", "-w", "0" });
    FileSystem fs = FileSystem.get(conf);

    FileStatus[] globStatus = fs.globStatus(new Path(TEST_OUTPUT + "/part-*"));
    JSONParser parser = new JSONParser();
    for (FileStatus fts : globStatus) {
      BufferedReader br = new BufferedReader(
          new InputStreamReader(fs.open(fts.getPath())));
      try {
        String line;
        line = br.readLine();

        while (line != null) {
          JSONArray jsonArray = (JSONArray)parser.parse(line);

          // the edge data begins at the third element.
          JSONArray edgeArray = (JSONArray)jsonArray.get(2);
          assertTrue(edgeArray.size() <= 10);

          for (Object obj : edgeArray) {
            JSONArray edge = (JSONArray)obj;
            assertTrue(Integer.parseInt(edge.get(0).toString()) < 20);
            assertTrue(Integer.parseInt(edge.get(0).toString()) >= 0);
            assertTrue(Integer.parseInt(edge.get(1).toString()) == 0);
          }
          line = br.readLine();
        }
      } finally {
        br.close();
      }
    }

    fs.delete(new Path(TEST_OUTPUT), true);
  }
}
