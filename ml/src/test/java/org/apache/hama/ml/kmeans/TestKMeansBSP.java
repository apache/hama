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
package org.apache.hama.ml.kmeans;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.ml.kmeans.KMeansBSP;
import org.apache.hama.ml.math.DoubleVector;

public class TestKMeansBSP extends TestCase {

  public void testRunJob() throws Exception {
    Configuration conf = new Configuration();
    Path in = new Path("/tmp/clustering/in/in.txt");
    Path out = new Path("/tmp/clustering/out/");
    FileSystem fs = FileSystem.get(conf);
    Path center = null;

    try {
      center = new Path(in.getParent(), "center/cen.seq");

      Path centerOut = new Path(out, "center/center_output.seq");
      conf.set(KMeansBSP.CENTER_IN_PATH, center.toString());
      conf.set(KMeansBSP.CENTER_OUT_PATH, centerOut.toString());
      int iterations = 10;
      conf.setInt(KMeansBSP.MAX_ITERATIONS_KEY, iterations);
      int k = 1;

      FSDataOutputStream create = fs.create(in);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(create));
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < 100; i++) {
        sb.append(i);
        sb.append('\t');
        sb.append(i);
        sb.append('\n');
      }

      bw.write(sb.toString());
      bw.close();

      in = KMeansBSP.prepareInputText(k, conf, in, center, out, fs);

      BSPJob job = KMeansBSP.createJob(conf, in, out, true);

      // just submit the job
      boolean result = job.waitForCompletion(true);

      assertEquals(true, result);

      HashMap<Integer, DoubleVector> centerMap = KMeansBSP.readOutput(conf,
          out, centerOut, fs);
      System.out.println(centerMap);
      assertEquals(1, centerMap.size());
      DoubleVector doubleVector = centerMap.get(0);
      assertTrue(doubleVector.get(0) >= 50 && doubleVector.get(0) < 51);
      assertTrue(doubleVector.get(1) >= 50 && doubleVector.get(1) < 51);
    } finally {
      fs.delete(new Path("/tmp/clustering"), true);
    }
  }

}
