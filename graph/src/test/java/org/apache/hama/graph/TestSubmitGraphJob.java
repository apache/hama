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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.TestBSPMasterGroomServer;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.graph.example.PageRank;
import org.apache.hama.graph.example.PageRank.PagerankSeqReader;

public class TestSubmitGraphJob extends TestBSPMasterGroomServer {

  String[] input = new String[] { "stackoverflow.com\tyahoo.com",
      "facebook.com\ttwitter.com\tgoogle.com\tnasa.gov",
      "yahoo.com\tnasa.gov\tstackoverflow.com",
      "twitter.com\tgoogle.com\tfacebook.com",
      "nasa.gov\tyahoo.com\tstackoverflow.com",
      "youtube.com\tgoogle.com\tyahoo.com", "google.com" };

  private static String INPUT = "/tmp/pagerank/real-tmp.seq";
  private static String OUTPUT = "/tmp/pagerank/real-out";

  @Override
  public void testSubmitJob() throws Exception {

    generateTestData();

    GraphJob bsp = new GraphJob(configuration, PageRank.class);
    bsp.setInputPath(new Path("/tmp/pagerank/real-tmp.seq"));
    bsp.setOutputPath(new Path(OUTPUT));
    BSPJobClient jobClient = new BSPJobClient(configuration);
    configuration.setInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 6000);
    configuration.set("hama.graph.self.ref", "true");
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    assertEquals(this.numOfGroom, cluster.getGroomServers());
    LOG.info("Client finishes execution job.");
    bsp.setJobName("Pagerank");
    bsp.setVertexClass(PageRank.PageRankVertex.class);
    // set the defaults
    bsp.setMaxIteration(30);

    bsp.setAggregatorClass(AverageAggregator.class);

    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setInputKeyClass(Text.class);
    bsp.setInputValueClass(TextArrayWritable.class);

    bsp.setVertexIDClass(Text.class);
    bsp.setVertexValueClass(DoubleWritable.class);
    bsp.setEdgeValueClass(NullWritable.class);
    bsp.setVertexInputReaderClass(PagerankSeqReader.class);

    bsp.setPartitioner(HashPartitioner.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);

    long startTime = System.currentTimeMillis();
    try {
      if (bsp.waitForCompletion(true)) {
        verifyResult();
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
            / 1000.0 + " seconds");
      } else {
        fail();
      }
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    double sum = 0.0;
    FileStatus[] globStatus = fs.globStatus(new Path(OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, fts.getPath(),
          configuration);
      Text key = new Text();
      DoubleWritable value = new DoubleWritable();

      while (reader.next(key, value)) {
        sum += value.get();
      }
      reader.close();
    }
    LOG.info("Sum is: " + sum);
    assertTrue("Sum was: " + sum, sum > 0.9d && sum <= 1.1d);
  }

  private void generateTestData() {
    try {
      SequenceFile.Writer writer1 = SequenceFile.createWriter(fs, getConf(),
          new Path(INPUT + "/part0"), Text.class, TextArrayWritable.class);

      for (int i = 0; i < input.length; i++) {
        String[] x = input[i].split("\t");

        Text vertex = new Text(x[0]);
        TextArrayWritable arr = new TextArrayWritable();
        Writable[] values = new Writable[x.length - 1];
        for (int j = 1; j < x.length; j++) {
          values[j - 1] = new Text(x[j]);
        }
        arr.set(values);
        writer1.append(vertex, arr);
      }

      writer1.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT).getParent()))
        fs.delete(new Path(INPUT).getParent(), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
