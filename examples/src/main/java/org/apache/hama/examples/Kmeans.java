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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.ml.kmeans.KMeansBSP;
import org.apache.hama.ml.writable.VectorWritable;

/**
 * Uses the {@link KMeansBSP} class to run a Kmeans Clustering with BSP. You can
 * provide your own input, or generate some random input for benchmarking.
 * 
 * For your own input, you can supply a text file that contains a tab separated
 * sequence of doubles on each line. The first k-vectors are used as the seed
 * centers.
 * 
 * For random input, just supply the "-g" command the number of vectors to
 * generate and the dimension of the vectors.
 * 
 * You must pass always an input directory and an output path, as well as how
 * many iterations the algorithm should run (it will also stop if the centers
 * won't move anymore).
 * 
 * The centers are stored in the given input path under
 * center/center_output.seq. This is a center sequencefile with
 * {@link VectorWritable} as key and {@link NullWritable} as value. You can read
 * it with the normal FS cat utility, but you have to add the hama-ml jar to the
 * lib directory of Hadoop, so it can find the vector classes.
 * 
 * The assignments from an index (the order of the center in the above sequence
 * file matters!, also starting from 0!) to a vector can be found in the output
 * path as text file.
 * 
 */
public class Kmeans {

  public static void main(String[] args) throws Exception {
    if (args.length < 4 || args.length != 7) {
      System.out
          .println("USAGE: <INPUT_PATH> <OUTPUT_PATH> <MAXITERATIONS> <K (how many centers)> -g [<COUNT> <DIMENSION OF VECTORS>]");
      return;
    }
    Configuration conf = new Configuration();
    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    Path center = null;
    if (fs.isFile(in))
      center = new Path(in.getParent(), "center/cen.seq");
    else
      center = new Path(in, "center/cen.seq");
    Path centerOut = new Path(out, "center/center_output.seq");
    conf.set(KMeansBSP.CENTER_IN_PATH, center.toString());
    conf.set(KMeansBSP.CENTER_OUT_PATH, centerOut.toString());
    int iterations = Integer.parseInt(args[2]);
    conf.setInt(KMeansBSP.MAX_ITERATIONS_KEY, iterations);
    int k = Integer.parseInt(args[3]);
    if (args.length == 7 && args[4].equals("-g")) {
      int count = Integer.parseInt(args[5]);
      if(k > count)
        throw new IllegalArgumentException("K can't be greater than n!");
      int dimension = Integer.parseInt(args[6]);
      System.out.println("N: " + count + " Dimension: " + dimension
          + " Iterations: " + iterations);
      // prepare the input, like deleting old versions and creating centers
      KMeansBSP.prepareInput(count, k, dimension, conf, in, center, out, fs);
    } else {
      KMeansBSP.prepareInputText(k, conf, in, center, out, fs);
      in = new Path(args[0], "textinput/in.seq");
    }

    BSPJob job = KMeansBSP.createJob(conf, in, out, true);

    // just submit the job
    job.waitForCompletion(true);
  }
}
