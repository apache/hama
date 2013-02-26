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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextArrayWritable;
import org.junit.Test;

/**
 * Testcase for {@link PageRank}
 */
public class PageRankTest extends TestCase {
  String[] input = new String[] { "1\t2\t3", "2", "3\t1\t2\t5", "4\t5\t6",
      "5\t4\t6", "6\t4", "7\t2\t4" };

  private static String INPUT = "/tmp/page-tmp.seq";
  private static String TEXT_INPUT = "/tmp/page.txt";
  private static String TEXT_OUTPUT = INPUT + "page.txt.seq";
  private static String OUTPUT = "/tmp/page-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testPageRank() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    generateTestData();
    try {
      PageRank.main(new String[] { INPUT, OUTPUT, "3" });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    double sum = 0d;
    for (FileStatus fts : globStatus) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          fs.open(fts.getPath())));
      String line = null;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        String[] split = line.split("\t");
        sum += Double.parseDouble(split[1]);
      }
    }
    System.out.println(sum);
    assertTrue("Sum was: " + sum, sum > 0.9 && sum < 1.1);
  }

  private void generateTestData() {
    try {
      SequenceFile.Writer writer1 = SequenceFile.createWriter(fs, conf,
          new Path(INPUT + "/part0"), Text.class, TextArrayWritable.class);

      for (int i = 0; i < input.length; i++) {
        String[] x = input[i].split("\t");

        Text vertex = new Text(x[0]);
        TextArrayWritable arr = new TextArrayWritable();
        Writable[] values = new Writable[x.length - 1];
        for (int j = 1; j < x.length; j++) {
          values[j - 1] = new Text(x[j]);
        }
        arr.set(values);
        writer1.append(vertex, arr);
      }

      writer1.close();

    } catch (IOException e) {
      e.printStackTrace();
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
