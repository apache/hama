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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.ml.regression.GradientDescentBSP;
import org.apache.hama.ml.regression.LogisticRegressionModel;
import org.apache.hama.ml.regression.RegressionModel;
import org.apache.hama.ml.regression.VectorDoubleFileInputFormat;
import org.apache.hama.ml.writable.VectorWritable;

import java.io.IOException;

/**
 * A {@link GradientDescentBSP} job example
 */
public class GradientDescentExample {
  private static final Path TMP_OUTPUT = new Path("/tmp/gd");

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {
    // BSP job configuration
    HamaConfiguration conf = new HamaConfiguration();
    conf.setFloat(GradientDescentBSP.ALPHA, 0.002f);
    conf.setFloat(GradientDescentBSP.COST_THRESHOLD, 0.5f);
    conf.setInt(GradientDescentBSP.ITERATIONS_THRESHOLD, 300);
    conf.setInt(GradientDescentBSP.INITIAL_THETA_VALUES, 10);
    if (args.length > 1 && args[1] != null && args[1].equals("logistic")) {
      conf.setClass(GradientDescentBSP.REGRESSION_MODEL_CLASS,
          LogisticRegressionModel.class, RegressionModel.class);
    }

    BSPJob bsp = new BSPJob(conf, GradientDescentExample.class);
    // Set the job name
    bsp.setJobName("Gradient Descent Example");
    bsp.setBspClass(GradientDescentBSP.class);
    bsp.setInputFormat(VectorDoubleFileInputFormat.class);
    bsp.setInputPath(new Path(args[0]));
    bsp.setInputKeyClass(VectorWritable.class);
    bsp.setInputValueClass(DoubleWritable.class);
    bsp.setOutputKeyClass(VectorWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT);

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      printOutput(conf);
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

  }

  static void printOutput(HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] files = fs.listStatus(TMP_OUTPUT);
    for (int i = 0; i < files.length; i++) {
      if (files[i].getLen() > 0) {
        FSDataInputStream in = fs.open(files[i].getPath());
        IOUtils.copyBytes(in, System.out, conf, false);
        in.close();
        break;
      }
    }

    fs.delete(TMP_OUTPUT, true);
  }
}
