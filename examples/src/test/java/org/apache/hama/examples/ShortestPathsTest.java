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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;

/**
 * Testcase for {@link ShortestPaths}
 */

public class ShortestPathsTest extends TestCase {
  private static String INPUT = "/tmp/sssp-tmp.seq";
  private static String OUTPUT = "/tmp/sssp-out";
  private Configuration conf;
  private FileSystem fs;

  public void testShortestPaths() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    conf = new HamaConfiguration();
    fs = FileSystem.get(conf);

    generateTestData();
    try {
      ShortestPaths.main(new String[] { "Frankfurt", OUTPUT, INPUT });

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

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT
        + "/part-00000"), conf);
    Text key = new Text();
    IntWritable value = new IntWritable();
    while (reader.next(key, value)) {
      assertEquals(value.get(), (int) rs.get(key.toString()));
    }
  }

  private void generateTestData() throws IOException {
    Map<ShortestPathVertex, ShortestPathVertexArrayWritable> tmp = new HashMap<ShortestPathVertex, ShortestPathVertexArrayWritable>();
    String[] cities = new String[] { "Frankfurt", "Mannheim", "Wuerzburg",
        "Stuttgart", "Kassel", "Karlsruhe", "Erfurt", "Nuernberg", "Augsburg",
        "Muenchen" };

    for (String city : cities) {
      if (city.equals("Frankfurt")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[3];
        textArr[0] = new ShortestPathVertex(85, "Mannheim");
        textArr[1] = new ShortestPathVertex(173, "Kassel");
        textArr[2] = new ShortestPathVertex(217, "Wuerzburg");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Stuttgart")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[1];
        textArr[0] = new ShortestPathVertex(183, "Nuernberg");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Kassel")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[2];
        textArr[0] = new ShortestPathVertex(502, "Muenchen");
        textArr[1] = new ShortestPathVertex(173, "Frankfurt");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Erfurt")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[1];
        textArr[0] = new ShortestPathVertex(186, "Wuerzburg");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Wuerzburg")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[3];
        textArr[0] = new ShortestPathVertex(217, "Frankfurt");
        textArr[1] = new ShortestPathVertex(186, "Erfurt");
        textArr[2] = new ShortestPathVertex(103, "Nuernberg");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Mannheim")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[2];
        textArr[0] = new ShortestPathVertex(80, "Karlsruhe");
        textArr[1] = new ShortestPathVertex(85, "Frankfurt");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Karlsruhe")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[2];
        textArr[0] = new ShortestPathVertex(250, "Augsburg");
        textArr[1] = new ShortestPathVertex(80, "Mannheim");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Augsburg")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[2];
        textArr[0] = new ShortestPathVertex(250, "Karlsruhe");
        textArr[1] = new ShortestPathVertex(84, "Muenchen");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Nuernberg")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[3];
        textArr[0] = new ShortestPathVertex(183, "Stuttgart");
        textArr[1] = new ShortestPathVertex(167, "Muenchen");
        textArr[2] = new ShortestPathVertex(103, "Wuerzburg");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      } else if (city.equals("Muenchen")) {
        ShortestPathVertex[] textArr = new ShortestPathVertex[3];
        textArr[0] = new ShortestPathVertex(167, "Nuernberg");
        textArr[1] = new ShortestPathVertex(502, "Kassel");
        textArr[2] = new ShortestPathVertex(84, "Augsburg");
        ShortestPathVertexArrayWritable arr = new ShortestPathVertexArrayWritable();
        arr.set(textArr);
        tmp.put(new ShortestPathVertex(0, city), arr);
      }
    }

    SequenceFile.Writer writer = SequenceFile
        .createWriter(fs, conf, new Path(INPUT), ShortestPathVertex.class,
            ShortestPathVertexArrayWritable.class);
    for (Map.Entry<ShortestPathVertex, ShortestPathVertexArrayWritable> e : tmp
        .entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
  }
  
  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
