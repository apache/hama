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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.ml.kcore.KCoreMessage;
import org.apache.hama.ml.kcore.KCoreVertex;
import org.apache.hama.ml.kcore.KCoreVertexReader;
import org.apache.hama.ml.kcore.KCoreVertexWriter;

public class KCore {
  private static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {
    GraphJob graphJob = new GraphJob(conf, KCore.class);
    graphJob.setJobName("KCore");

    graphJob.setInputPath(new Path(args[0]));
    graphJob.setOutputPath(new Path(args[1]));

    graphJob.setVertexClass(KCoreVertex.class);
    graphJob.setVertexIDClass(LongWritable.class);
    graphJob.setEdgeValueClass(LongWritable.class);
    graphJob.setVertexValueClass(KCoreMessage.class);

    graphJob.setVertexInputReaderClass(KCoreVertexReader.class);
    graphJob.setVertexOutputWriterClass(KCoreVertexWriter.class);

    graphJob.setOutputKeyClass(LongWritable.class);
    graphJob.setOutputValueClass(IntWritable.class);

    graphJob.setInputFormat(TextInputFormat.class);
    graphJob.setInputKeyClass(LongWritable.class);
    graphJob.setInputValueClass(Text.class);
    
    return graphJob;
  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output>");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      printUsage();
    }
    HamaConfiguration conf = new HamaConfiguration(new Configuration());
    GraphJob graphJob = createJob(args, conf);
    long startTime = System.currentTimeMillis();
    if (graphJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
