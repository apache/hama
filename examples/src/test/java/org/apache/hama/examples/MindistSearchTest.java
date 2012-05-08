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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.MindistSearch.MinTextCombiner;
import org.apache.hama.examples.util.PagerankTextToSeq;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

public class MindistSearchTest extends TestCase {

  private static final Map<VertexWritable, VertexArrayWritable> tmp = new HashMap<VertexWritable, VertexArrayWritable>();
  // mapping of our index of the vertex to the resulting component id
  private static final String[] resultList = new String[] { "0", "1", "2", "2",
      "1", "2", "2", "1", "2", "0" };
  static {
    String[] pages = new String[] { "0", "1", "2", "3", "4", "5", "6", "7",
        "8", "9" };
    String[] lineArray = new String[] { "0", "1;4;7", "2;3;8", "3;5", "4;1",
        "5;6", "6", "7", "8;3", "9;0" };

    for (int i = 0; i < lineArray.length; i++) {
      String[] adjacencyStringArray = lineArray[i].split(";");
      int vertexId = Integer.parseInt(adjacencyStringArray[0]);
      String name = pages[vertexId];
      VertexWritable[] arr = new VertexWritable[adjacencyStringArray.length - 1];
      for (int j = 1; j < adjacencyStringArray.length; j++) {
        arr[j - 1] = new VertexWritable(
            pages[Integer.parseInt(adjacencyStringArray[j])]);
      }
      VertexArrayWritable wr = new VertexArrayWritable();
      wr.set(arr);
      tmp.put(new VertexWritable(name), wr);
    }
  }
  private static String INPUT = "/tmp/pagerank-tmp.seq";
  private static String TEXT_INPUT = "/tmp/pagerank.txt";
  private static String TEXT_OUTPUT = INPUT + "pagerank.txt.seq";
  private static String OUTPUT = "/tmp/pagerank-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  public void testPageRank() throws Exception {
    generateSeqTestData();
    try {
      MindistSearch.main(new String[] { INPUT, OUTPUT });

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
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT
        + "/part-00000"), conf);
    Text key = new Text();
    Writable value = new Text();
    while (reader.next(key, value)) {
      System.out.println(key + " | " + value);
      assertEquals(resultList[Integer.parseInt(key.toString())],
          value.toString());
    }
  }

  private void generateSeqTestData() throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(
        INPUT), VertexWritable.class, VertexArrayWritable.class);
    for (Map.Entry<VertexWritable, VertexArrayWritable> e : tmp.entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
  }

  public void testPageRankUtil() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    generateTestTextData();
    // <input path> <output path>
    PagerankTextToSeq.main(new String[] { TEXT_INPUT, TEXT_OUTPUT });
    try {
      MindistSearch.main(new String[] { TEXT_OUTPUT, OUTPUT });

      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void generateTestTextData() throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(TEXT_INPUT));
    for (Map.Entry<VertexWritable, VertexArrayWritable> e : tmp.entrySet()) {
      writer.write(e.getKey() + "\t");
      for (int i = 0; i < e.getValue().get().length; i++) {
        VertexWritable writable = (VertexWritable) e.getValue().get()[i];
        writer.write(writable.getName() + "\t");
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
