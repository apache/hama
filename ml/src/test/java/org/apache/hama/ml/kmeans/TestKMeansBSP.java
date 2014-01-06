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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.commons.math.DoubleVector;

public class TestKMeansBSP extends TestCase {
  public static final String TMP_OUTPUT = "/tmp/clustering/";

  public void testRunJob() throws Exception {
    Configuration conf = new HamaConfiguration();
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(new Path(TMP_OUTPUT))) {
      fs.delete(new Path(TMP_OUTPUT), true);
    }

    // test for bspTaskNum 1 to 4
    for (int i = 1; i < 5; i++) {
      try {
        test(conf, fs, i);
      } finally {
        fs.delete(new Path(TMP_OUTPUT), true);
      }
    }
  }

  /**
   * Test
   * 
   * Create 101 input vectors of dimension two
   * 
   * Input vectors: (0,0) (1,1) (2,2) ... (100,100)
   * 
   * k = 1, maxIterations = 10
   * 
   * Resulting center should be (50,50)
   */
  private void test(Configuration conf, FileSystem fs, int numBspTask)
      throws IOException, InterruptedException, ClassNotFoundException {

    Path in = new Path(TMP_OUTPUT + "in");
    Path out = new Path(TMP_OUTPUT + "out");
    Path centerIn = new Path(TMP_OUTPUT + "center/center_input.seq");
    Path centerOut = new Path(TMP_OUTPUT + "center/center_output.seq");
    conf.set(KMeansBSP.CENTER_IN_PATH, centerIn.toString());
    conf.set(KMeansBSP.CENTER_OUT_PATH, centerOut.toString());

    int k = 1;
    int iterations = 10;
    conf.setInt(KMeansBSP.MAX_ITERATIONS_KEY, iterations);

    in = generateInputText(k, conf, fs, in, centerIn, out, numBspTask);

    BSPJob job = KMeansBSP.createJob(conf, in, out, true);
    job.setNumBspTask(numBspTask);

    // just submit the job
    boolean result = job.waitForCompletion(true);

    assertEquals(true, result);

    HashMap<Integer, DoubleVector> centerMap = KMeansBSP.readClusterCenters(
        conf, out, centerOut, fs);
    System.out.println(centerMap);

    assertEquals(1, centerMap.size()); // because k = 1

    DoubleVector doubleVector = centerMap.get(0);
    assertEquals(Double.valueOf(50), doubleVector.get(0));
    assertEquals(Double.valueOf(50), doubleVector.get(1));
  }

  private Path generateInputText(int k, Configuration conf, FileSystem fs,
      Path in, Path centerIn, Path out, int numBspTask) throws IOException {
    int totalNumberOfPoints = 100;
    int interval = totalNumberOfPoints / numBspTask;
    Path parts = new Path(in, "parts");

    for (int part = 0; part < numBspTask; part++) {
      Path partIn = new Path(parts, "part" + part + "/input.txt");
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
          fs.create(partIn)));

      int start = interval * part;
      int end = start + interval - 1;
      if ((numBspTask - 1) == part) {
        end = totalNumberOfPoints;
      }
      System.out
          .println("Partition " + part + ": from " + start + " to " + end);

      for (int i = start; i <= end; i++) {
        bw.append(i + "\t" + i + "\n");
      }
      bw.close();

      // Convert input text to sequence file
      Path seqFile = null;
      if (part == 0) {
        seqFile = KMeansBSP.prepareInputText(k, conf, partIn, centerIn, out,
            fs, false);
      } else {
        seqFile = KMeansBSP.prepareInputText(0, conf, partIn, new Path(centerIn
            + "_empty.seq"), out, fs, false);
      }

      fs.moveFromLocalFile(seqFile, new Path(parts, "part" + part + ".seq"));
      fs.delete(seqFile.getParent(), true);
      fs.delete(partIn.getParent(), true);
    }

    return parts;
  }

}
