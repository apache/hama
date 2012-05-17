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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.examples.PageRank.PageRankVertex;
import org.apache.hama.examples.util.PagerankTextToSeq;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.GraphJobRunner;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

public class PageRankTest extends TestCase {
  /**
   * The graph looks like this (adjacency list, [] contains outlinks):<br/>
   * stackoverflow.com [yahoo.com] <br/>
   * google.com []<br/>
   * facebook.com [twitter.com, google.com, nasa.gov]<br/>
   * yahoo.com [nasa.gov, stackoverflow.com]<br/>
   * twitter.com [google.com, facebook.com]<br/>
   * nasa.gov [yahoo.com, stackoverflow.com]<br/>
   * youtube.com [google.com, yahoo.com]<br/>
   */
  private static final Map<VertexWritable, VertexArrayWritable> tmp = new HashMap<VertexWritable, VertexArrayWritable>();
  static {
    // our first entry is null, because our indices in hama 3.0 pre calculated
    // example starts at 1.
    // FIXME This is really ugly.
    String[] pages = new String[] { null, "twitter.com", "google.com",
        "facebook.com", "yahoo.com", "nasa.gov", "stackoverflow.com",
        "youtube.com" };
    String[] lineArray = new String[] { "1;2;3", "2", "3;1;2;5", "4;5;6",
        "5;4;6", "6;4", "7;2;4" };

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
    generateSeqTestData(tmp);
    try {
      // Usage: <input> <output> [damping factor (default 0.85)] [Epsilon
      // (convergence error, default 0.001)] [Max iterations (default 30)]
      // [tasks]
      PageRank.main(new String[] { INPUT, OUTPUT, "0.85", "0.0001", "-1" });

      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT
        + "/part-00000"), conf);
    Text key = new Text();
    DoubleWritable value = new DoubleWritable();
    double sum = 0.0;
    while (reader.next(key, value)) {
      sum += value.get();
    }
    System.out.println("Sum is: " + sum);
    assertTrue(sum > 0.99d && sum <= 1d);
  }

  private void generateSeqTestData(Map<VertexWritable, VertexArrayWritable> tmp)
      throws IOException {
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
      PageRank
          .main(new String[] { TEXT_OUTPUT, OUTPUT, "0.85", "0.0001", "-1" });

      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  public void testRepairFunctionality() throws Exception {
    // make a copy to be safe with parallel test executions
    final Map<VertexWritable, VertexArrayWritable> map = new HashMap<VertexWritable, VertexArrayWritable>(
        tmp);
    // removing google should resulting in creating it and getting the same
    // result as usual
    map.remove(new VertexWritable("google.com"));
    generateSeqTestData(map);
    try {
      HamaConfiguration conf = new HamaConfiguration(new Configuration());
      conf.setBoolean(GraphJobRunner.GRAPH_REPAIR, true);
      GraphJob pageJob = new GraphJob(conf, PageRank.class);
      pageJob.setJobName("Pagerank");

      pageJob.setVertexClass(PageRankVertex.class);
      pageJob.setInputPath(new Path(INPUT));
      pageJob.setOutputPath(new Path(OUTPUT));

      // set the defaults
      pageJob.setMaxIteration(30);
      pageJob.set("hama.pagerank.alpha", "0.85");
      // we need to include a vertex in its adjacency list,
      // otherwise the pagerank result has a constant loss
      pageJob.set("hama.graph.self.ref", "true");

      pageJob.setAggregatorClass(AverageAggregator.class);

      pageJob.setInputFormat(SequenceFileInputFormat.class);
      pageJob.setPartitioner(HashPartitioner.class);
      pageJob.setOutputFormat(SequenceFileOutputFormat.class);
      pageJob.setOutputKeyClass(Text.class);
      pageJob.setOutputValueClass(DoubleWritable.class);

      if (!pageJob.waitForCompletion(true)) {
        fail("Job did not complete normally!");
      }
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
