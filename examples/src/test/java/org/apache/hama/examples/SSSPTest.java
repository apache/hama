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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

/**
 * Testcase for {@link ShortestPaths}
 */

@SuppressWarnings("unchecked")
public class SSSPTest extends TestCase {

  private static final Map<VertexWritable<Text, IntWritable>, VertexArrayWritable> testData = new HashMap<VertexWritable<Text, IntWritable>, VertexArrayWritable>();

  static {
    Configuration conf = new Configuration();
    VertexWritable.CONFIGURATION = conf;
    String[] cities = new String[] { "Frankfurt", "Mannheim", "Wuerzburg",
        "Stuttgart", "Kassel", "Karlsruhe", "Erfurt", "Nuernberg", "Augsburg",
        "Muenchen" };

    for (String city : cities) {
      if (city.equals("Frankfurt")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable<Text, IntWritable>(85, "Mannheim");
        textArr[1] = new VertexWritable<Text, IntWritable>(173, "Kassel");
        textArr[2] = new VertexWritable<Text, IntWritable>(217, "Wuerzburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Stuttgart")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[1];
        textArr[0] = new VertexWritable<Text, IntWritable>(183, "Nuernberg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Kassel")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable<Text, IntWritable>(502, "Muenchen");
        textArr[1] = new VertexWritable<Text, IntWritable>(173, "Frankfurt");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Erfurt")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[1];
        textArr[0] = new VertexWritable<Text, IntWritable>(186, "Wuerzburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Wuerzburg")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable<Text, IntWritable>(217, "Frankfurt");
        textArr[1] = new VertexWritable<Text, IntWritable>(186, "Erfurt");
        textArr[2] = new VertexWritable<Text, IntWritable>(103, "Nuernberg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Mannheim")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable<Text, IntWritable>(80, "Karlsruhe");
        textArr[1] = new VertexWritable<Text, IntWritable>(85, "Frankfurt");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Karlsruhe")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable<Text, IntWritable>(250, "Augsburg");
        textArr[1] = new VertexWritable<Text, IntWritable>(80, "Mannheim");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Augsburg")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable<Text, IntWritable>(250, "Karlsruhe");
        textArr[1] = new VertexWritable<Text, IntWritable>(84, "Muenchen");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Nuernberg")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable<Text, IntWritable>(183, "Stuttgart");
        textArr[1] = new VertexWritable<Text, IntWritable>(167, "Muenchen");
        textArr[2] = new VertexWritable<Text, IntWritable>(103, "Wuerzburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      } else if (city.equals("Muenchen")) {
        VertexWritable<Text, IntWritable>[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable<Text, IntWritable>(167, "Nuernberg");
        textArr[1] = new VertexWritable<Text, IntWritable>(502, "Kassel");
        textArr[2] = new VertexWritable<Text, IntWritable>(84, "Augsburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable<Text, IntWritable>(0, city), arr);
      }
    }
  }

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

    generateTestSequenceFileData();
    try {
      SSSP.main(new String[] { "Frankfurt", INPUT, OUTPUT });

      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    Map<String, Integer> rs = new HashMap<String, Integer>();
    rs.put("Erfurt", 403);
    rs.put("Mannheim", 85);
    rs.put("Stuttgart", 503);
    rs.put("Kassel", 173);
    rs.put("Nuernberg", 320);
    rs.put("Augsburg", 415);
    rs.put("Frankfurt", 0);
    rs.put("Muenchen", 487);
    rs.put("Wuerzburg", 217);
    rs.put("Karlsruhe", 165);

    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, fts.getPath(),
          conf);
      Text key = new Text();
      IntWritable value = new IntWritable();
      while (reader.next(key, value)) {
        assertEquals(value.get(), (int) rs.get(key.toString()));
      }
    }
  }

  private void generateTestSequenceFileData() throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(
        INPUT), VertexWritable.class, VertexArrayWritable.class);
    for (Map.Entry<VertexWritable<Text, IntWritable>, VertexArrayWritable> e : testData
        .entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
  }

  @SuppressWarnings("unused")
  private static void generateTestTextData() throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(TEXT_INPUT));
    for (Map.Entry<VertexWritable<Text, IntWritable>, VertexArrayWritable> e : testData
        .entrySet()) {
      writer.write(e.getKey().getVertexId() + "\t");
      for (int i = 0; i < e.getValue().get().length; i++) {
        writer
            .write(((VertexWritable<Text, IntWritable>) e.getValue().get()[i])
                .getVertexId()
                + ":"
                + ((VertexWritable<Text, IntWritable>) e.getValue().get()[i])
                    .getVertexValue() + "\t");
      }
      writer.write("\n");
    }
    writer.close();
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
