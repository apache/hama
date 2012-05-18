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
package org.apache.hama.examples.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

public class PagerankTextToSeqTest extends TestCase {

  private static final String DELIMITER = ";";
  private static final String TXT_INPUT_DIR = "/tmp/pageranktext/";
  private static final String TXT_INPUT = TXT_INPUT_DIR + "in.txt";
  private static final String SEQ_OUTPUT = "/tmp/pageranktext/";
  private static final String SEQ_INPUT = SEQ_OUTPUT + "in.txt.seq";

  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
    deleteTempDirs();
    File dir = new File(TXT_INPUT_DIR);
    if (!dir.exists()) {
      dir.mkdirs();
    }
  }

  private static void writeTextFile() throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(TXT_INPUT));
    for (int lines = 0; lines < 10; lines++) {
      for (int cols = 0; cols < 5; cols++) {
        writer.append(cols + DELIMITER);
      }
      writer.append("\n");
    }
    writer.close();
  }

  @SuppressWarnings("unchecked")
  private void verifyOutput() throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs,
        new Path(SEQ_INPUT), conf);
    VertexWritable<Text, DoubleWritable> vertex = new VertexWritable<Text, DoubleWritable>();
    VertexArrayWritable vertexArray = new VertexArrayWritable();

    while (reader.next(vertex, vertexArray)) {
      int count = 0;
      assertEquals(vertex.getVertexId().toString(), count + "");
      Writable[] writables = vertexArray.get();
      assertEquals(writables.length, 4);
      for (int i = 0; i < 4; i++) {
        count++;
        assertEquals(((VertexWritable<Text, DoubleWritable>) writables[i])
            .getVertexId().toString(), count + "");
      }
    }
    reader.close();
  }

  public void testArgs() throws Exception {
    writeTextFile();
    PagerankTextToSeq.main(new String[] { TXT_INPUT, SEQ_OUTPUT, DELIMITER });
    verifyOutput();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    deleteTempDirs();
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(TXT_INPUT_DIR)))
        fs.delete(new Path(TXT_INPUT_DIR), true);
      if (fs.exists(new Path(TXT_INPUT)))
        fs.delete(new Path(TXT_INPUT), true);
      if (fs.exists(new Path(SEQ_OUTPUT)))
        fs.delete(new Path(SEQ_OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
