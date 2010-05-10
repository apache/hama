/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hama.Constants;
import org.apache.hama.HamaAdmin;
import org.apache.hama.HamaAdminImpl;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.mapreduce.MatrixNormMapReduce;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.matrix.Matrix.Norm;

public class MatrixNorm extends AbstractExample {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("norm [-m maps] [-r reduces] <matrix name> <type>");
      System.out.println("arguments: type - one | infinity | frobenius | maxvalue");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    HamaAdmin admin = new HamaAdminImpl(conf);
    Matrix a = admin.getMatrix(ARGS.get(0));
    if(ARGS.get(1).equalsIgnoreCase("one")) {
      System.out.println("The maximum absolute column sum of matrix '" + ARGS.get(0)
          + "' is " + norm(a, Norm.One));
    } else if(ARGS.get(1).equalsIgnoreCase("infinity")) {
      System.out.println("The maximum absolute row sum of matrix '" + ARGS.get(0)
          + "' is " + norm(a, Norm.Infinity));
    } else if(ARGS.get(1).equalsIgnoreCase("frobenius")) {
      System.out.println("The root of sum of squares of matrix '" + ARGS.get(0)
          + "' is " + norm(a, Norm.Frobenius));
    } else {
      System.out.println("The max absolute cell value of matrix '" + ARGS.get(0)
          + "' is " + norm(a, Norm.Maxvalue));
    }
  }

  /**
   * Computes the given norm of the matrix
   * 
   * @param type
   * @return norm of the matrix
   * @throws IOException
   */
  public static double norm(Matrix a, Norm type) throws IOException {
    if (type == Norm.One)
      return getNorm1(a);
    else if (type == Norm.Frobenius)
      return getFrobenius(a);
    else if (type == Norm.Infinity)
      return getInfinity(a);
    else
      return getMaxvalue(a);
  }
  

  public static double getNorm1(Matrix a) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    Path outDir = new Path(new Path(a.getType() + "_TMP_norm1_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(conf, "norm1 MR job : " + a.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(a.getPath(), scan,
        MatrixNormMapReduce.MatrixOneNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixOneNormCombiner.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixOneNormReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
      System.out.println(job.reduceProgress());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(conf, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  protected static double getMaxvalue(Matrix a) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    Path outDir = new Path(new Path(a.getType() + "_TMP_normMaxValue_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(conf, "MaxValue Norm MR job : " + a.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(a.getPath(), scan,
        MatrixNormMapReduce.MatrixMaxValueNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixMaxValueNormReducer.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixMaxValueNormReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(conf, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  protected static double getInfinity(Matrix a) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    Path outDir = new Path(new Path(a.getType() + "_TMP_normInifity_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(conf, "Infinity Norm MR job : " + a.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(a.getPath(), scan,
        MatrixNormMapReduce.MatrixInfinityNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixInfinityNormReduce.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixInfinityNormReduce.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(conf, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  protected static double getFrobenius(Matrix a) throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    Path outDir = new Path(new Path(a.getType() + "_TMP_normFrobenius_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(conf, "Frobenius Norm MR job : " + a.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(a.getPath(), scan,
        MatrixNormMapReduce.MatrixFrobeniusNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixFrobeniusNormCombiner.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixFrobeniusNormReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(conf, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  public static double readOutput(HamaConfiguration config, FileSystem fs, Path outDir)
      throws IOException {
    Path inFile = new Path(outDir, "part-r-00000");
    IntWritable numInside = new IntWritable();
    DoubleWritable result = new DoubleWritable();
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, config);
    try {
      reader.next(numInside, result);
    } finally {
      reader.close();
    }
    return result.get();
  }
}
