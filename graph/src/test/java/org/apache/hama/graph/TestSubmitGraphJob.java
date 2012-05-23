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
package org.apache.hama.graph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TestBSPMasterGroomServer;
import org.apache.hama.graph.example.PageRank;

public class TestSubmitGraphJob extends TestBSPMasterGroomServer {

  private static final Map<VertexWritable<Text, DoubleWritable>, VertexArrayWritable> tmp = new HashMap<VertexWritable<Text, DoubleWritable>, VertexArrayWritable>();
  static {
    Configuration conf = new HamaConfiguration();
    VertexWritable.CONFIGURATION = conf;
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
      @SuppressWarnings("unchecked")
      VertexWritable<Text, NullWritable>[] arr = new VertexWritable[adjacencyStringArray.length - 1];
      for (int j = 1; j < adjacencyStringArray.length; j++) {
        arr[j - 1] = new VertexWritable<Text, NullWritable>(NullWritable.get(),
            new Text(pages[Integer.parseInt(adjacencyStringArray[j])]),
            Text.class, NullWritable.class);
      }
      VertexArrayWritable wr = new VertexArrayWritable();
      wr.set(arr);
      tmp.put(new VertexWritable<Text, DoubleWritable>(name), wr);
    }
  }

  private static String INPUT = "/tmp/pagerank-real-tmp.seq";
  private static String OUTPUT = "/tmp/pagerank-real-out";

  @SuppressWarnings("unchecked")
  @Override
  public void testSubmitJob() throws Exception {

    generateSeqTestData(tmp);

    GraphJob bsp = new GraphJob(configuration, PageRank.class);
    bsp.setInputPath(new Path(INPUT));
    bsp.setOutputPath(new Path(OUTPUT));
    BSPJobClient jobClient = new BSPJobClient(configuration);
    configuration.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 6000);
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    assertEquals(this.numOfGroom, cluster.getGroomServers());
    bsp.setNumBspTask(2);
    LOG.info("Client finishes execution job.");
    bsp.setJobName("Pagerank");
    bsp.setVertexClass(PageRank.PageRankVertex.class);
    // set the defaults
    bsp.setMaxIteration(30);
    bsp.set("hama.pagerank.alpha", "0.85");
    // we need to include a vertex in its adjacency list,
    // otherwise the pagerank result has a constant loss
    bsp.set("hama.graph.self.ref", "true");
    bsp.setAggregatorClass(AverageAggregator.class, SumAggregator.class);

    bsp.setVertexIDClass(Text.class);
    bsp.setVertexValueClass(DoubleWritable.class);
    bsp.setEdgeValueClass(NullWritable.class);

    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setPartitioner(HashPartitioner.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      verifyResult();
      LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
          / 1000.0 + " seconds");
    } else {
      fail();
    }
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
    LOG.info("Sum is: " + sum);
    assertTrue(sum > 0.99d && sum <= 1.1d);
  }

  private void generateSeqTestData(
      Map<VertexWritable<Text, DoubleWritable>, VertexArrayWritable> tmp)
      throws IOException {
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(
        INPUT), VertexWritable.class, VertexArrayWritable.class);
    for (Entry<VertexWritable<Text, DoubleWritable>, VertexArrayWritable> e : tmp
        .entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
  }
}
