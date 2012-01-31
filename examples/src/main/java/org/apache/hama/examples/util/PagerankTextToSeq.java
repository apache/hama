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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;
import org.apache.hama.util.KeyValuePair;

/**
 * This utility turns a Pagerank-formatted text file into a sequence file that
 * can be inputted to the Pagerank example. <br/>
 * 
 * <pre>
 * Usage: &lt;input path&gt; &lt;output path&gt; &lt;optional: text separator. Default is \"\t\"&gt;
 * 
 * </pre>
 * 
 * So you may start this with: <br/>
 * 
 * <pre>
 *    bin/hama -jar examples.jar pagerank-text2seq /tmp/in /tmp/out ";"
 * </pre>
 */
public class PagerankTextToSeq extends TextToSequenceFile {

  public PagerankTextToSeq(Path inPath, Path outPath, String delimiter)
      throws IOException {
    super(inPath, outPath, delimiter);
  }

  @Override
  protected KeyValuePair<VertexWritable, VertexArrayWritable> processLine(
      String line) throws IOException {
    String[] split = line.split(delimiter);
    VertexWritable key = new VertexWritable(split[0]);
    VertexWritable[] v = new VertexWritable[split.length - 1];
    for (int i = 1; i < split.length; i++) {
      v[i - 1] = new VertexWritable(split[i]);
    }
    VertexArrayWritable value = new VertexArrayWritable();
    value.set(v);
    return new KeyValuePair<VertexWritable, VertexArrayWritable>(key, value);
  }

  @Override
  protected Writer getWriter(Path outPath) throws IOException {
    return new Writer(destFs, conf, outPath, VertexWritable.class,
        VertexArrayWritable.class);
  }

  private static void printUsage() {
    LOG.info("<input path> <output path> <optional: text separator. Default is \"\t\">");
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2 && args.length != 3) {
      printUsage();
      System.exit(-1);
    }

    String inPath = args[0];
    String outPath = args[1];
    String delimiter = "\t";
    if (args.length > 2) {
      delimiter = args[2];
    }
    PagerankTextToSeq o = new PagerankTextToSeq(new Path(inPath), new Path(
        outPath), delimiter);
    o.run();
  }
}
