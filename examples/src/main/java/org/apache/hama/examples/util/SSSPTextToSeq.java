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
 * This utility turns a SSSP (Single Source Shortest Paths)-formatted text file
 * into a sequence file that can be inputted to the SSSP example. <br/>
 * 
 * <pre>
 * Usage: &lt;input path&gt; &lt;output path&gt; &lt;optional: text separator. Default is \"\t\"&gt;
 * &lt;optional: edge weight separator. Default is \":\"&gt;
 * 
 * </pre>
 * 
 * So you may start this with: <br/>
 * 
 * <pre>
 *    bin/hama -jar examples.jar sssp-text2seq /tmp/in /tmp/out ";" ":"
 * </pre>
 */
public class SSSPTextToSeq extends TextToSequenceFile {

  private final String edgeDelimiter;

  public SSSPTextToSeq(Path inPath, Path outPath, String delimiter,
      String edgeDelimiter) throws IOException {
    super(inPath, outPath, delimiter);
    this.edgeDelimiter = edgeDelimiter;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected KeyValuePair<VertexWritable, VertexArrayWritable> processLine(
      String line) throws IOException {
    String[] split = line.split(delimiter);
    VertexWritable key = new VertexWritable(0, split[0]);
    VertexWritable[] v = new VertexWritable[split.length - 1];
    for (int i = 1; i < split.length; i++) {
      String[] weightSplit = split[i].split(edgeDelimiter);
      if (weightSplit.length != 2) {
        LOG.error("Adjacent vertices must contain a \"" + edgeDelimiter
            + "\" between the vertex name and the edge weight! Line was: "
            + line);
      }
      v[i - 1] = new VertexWritable(Integer.parseInt(weightSplit[1]),
          weightSplit[0]);
    }
    VertexArrayWritable value = new VertexArrayWritable();
    value.set(v);
    return new KeyValuePair(key, value);
  }

  @Override
  protected Writer getWriter(Path outPath) throws IOException {
    return new Writer(destFs, conf, outPath, VertexWritable.class,
        VertexArrayWritable.class);
  }

  private static void printUsage() {
    LOG.info("<input path> <output path> <optional: text separator. Default is \"\t\"> <optional: edge weight separator. Default is \":\">");
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2 && args.length != 3 && args.length != 4) {
      printUsage();
      System.exit(-1);
    }

    String inPath = args[0];
    String outPath = args[1];
    String delimiter = "\t";
    if (args.length > 2) {
      delimiter = args[2];
    }
    String edgeDelimiter = ":";
    if (args.length > 3) {
      edgeDelimiter = args[3];
    }
    SSSPTextToSeq o = new SSSPTextToSeq(new Path(inPath), new Path(outPath),
        delimiter, edgeDelimiter);
    o.run();
  }
}
