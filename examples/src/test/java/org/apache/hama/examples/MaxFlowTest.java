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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.commons.io.FloatArrayWritable;
import org.junit.Test;

/**
 * Testcase for {@link MaxFlow}
 */
public class MaxFlowTest extends TestCase {
  private static String INPUT = "/tmp/maxflow-tmp.seq";
  private static String TEXT_INPUT = "/tmp/maxflow.txt";
  private static String TEXT_OUTPUT = INPUT + "maxflow.txt.seq";
  private static String OUTPUT = "/tmp/maxflow-output";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  private static String[] input = new String[] {

  "0\t1\t16", "1\t0\t-16", "1\t3\t12", "3\t1\t-12", "3\t5\t20", "5\t3\t-20",
      "2\t1\t4", "1\t2\t-4", "3\t2\t9", "2\t3\t-9", "4\t3\t7", "3\t4\t-7",
      "0\t2\t13", "2\t0\t-13", "2\t4\t14", "4\t2\t-14", "4\t5\t4", "5\t4\t-4"

  };

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testMaxFlow() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    generateTestData();
    try {
      MaxFlow.main(new String[] { INPUT, OUTPUT, "3" });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    File f = new File(OUTPUT + "/maxflow");
    InputStreamReader in = new InputStreamReader(new FileInputStream(f));
    @SuppressWarnings("resource")
    BufferedReader br = new BufferedReader(in);
    String result = br.readLine();
    System.out.println("maxflow is: " + String.valueOf(23.0));
    assertTrue(result.equals(String.valueOf(23.0)));
  }

  private void generateTestData() {

    Configuration conf = new Configuration();
    FileSystem fs;
    SequenceFile.Writer writer = null;
    try {
      fs = FileSystem.get(conf);
      Path path = new Path(INPUT);
      writer = SequenceFile.createWriter(fs, conf, path, FloatWritable.class,
          FloatArrayWritable.class);

      for (String s : input) {
        FloatArrayWritable value = new FloatArrayWritable();
        FloatWritable[] valueArray = new FloatWritable[2];
        value.set(valueArray);
        String[] array = s.split("\t");
        FloatWritable key = new FloatWritable(Float.valueOf(array[0]));
        valueArray[0] = new FloatWritable(Float.valueOf(array[1])); // store v2.
        valueArray[1] = new FloatWritable(Float.valueOf(array[2]));
        System.out.println(" " + Float.valueOf(array[0]) + " "
            + Float.valueOf(array[1]) + " " + Float.valueOf(array[2]));
        writer.append(key, value);
      }
      writer.close();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
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
