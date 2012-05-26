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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.examples.PageRank.PageRankVertex;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.GraphJobRunner;

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
   * Note that google is removed in this part mainly to test the repair
   * functionality.
   */
  String[] input = new String[] { "stackoverflow.com\tyahoo.com",
      "facebook.com\ttwitter.com\tgoogle.com\tnasa.gov]",
      "yahoo.com\tnasa.gov\tstackoverflow.com]",
      "twitter.com\tgoogle.com\tfacebook.com]",
      "nasa.gov\tyahoo.com\tstackoverflow.com]",
      "youtube.com\tgoogle.com\tyahoo.com]" };

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

  private void verifyResult() throws IOException {
    double sum = 0.0;
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, fts.getPath(),
          conf);
      Text key = new Text();
      DoubleWritable value = new DoubleWritable();

      while (reader.next(key, value)) {
        sum += value.get();
      }
    }
    System.out.println("Sum is: " + sum);
    assertTrue(sum > 0.99d && sum <= 1.1d);
  }

  public void testRepairFunctionality() throws Exception {
    generateTestData();
    try {
      HamaConfiguration conf = new HamaConfiguration(new Configuration());
      conf.setBoolean(GraphJobRunner.GRAPH_REPAIR, true);
      GraphJob pageJob = new GraphJob(conf, PageRank.class);
      pageJob.setJobName("Pagerank");

      pageJob.setVertexClass(PageRankVertex.class);
      pageJob.setInputPath(new Path(INPUT));
      pageJob.setOutputPath(new Path(OUTPUT));
      pageJob.setNumBspTask(2);
      // set the defaults
      pageJob.setMaxIteration(30);
      pageJob.set("hama.pagerank.alpha", "0.85");
      // we need to include a vertex in its adjacency list,
      // otherwise the pagerank result has a constant loss
      pageJob.set("hama.graph.self.ref", "true");

      pageJob.setAggregatorClass(AverageAggregator.class);
      pageJob.setInputKeyClass(LongWritable.class);
      pageJob.setInputValueClass(Text.class);
      pageJob.setInputFormat(TextInputFormat.class);
      pageJob.setPartitioner(HashPartitioner.class);
      pageJob.setOutputFormat(SequenceFileOutputFormat.class);
      pageJob.setOutputKeyClass(Text.class);
      pageJob.setOutputValueClass(DoubleWritable.class);
      pageJob.setVertexInputReaderClass(PageRank.PagerankTextReader.class);
      pageJob.setVertexIDClass(Text.class);
      pageJob.setVertexValueClass(DoubleWritable.class);
      pageJob.setEdgeValueClass(NullWritable.class);

      if (!pageJob.waitForCompletion(true)) {
        fail("Job did not complete normally!");
      }
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void generateTestData() {
    BufferedWriter bw = null;
    try {
      bw = new BufferedWriter(new FileWriter(INPUT));
      for (String s : input) {
        bw.write(s + "\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
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
