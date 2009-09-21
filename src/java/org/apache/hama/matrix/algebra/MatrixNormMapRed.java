/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hama.matrix.algebra;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hama.Constants;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.mapred.VectorInputFormat;

/** A Catalog class collect all the mr classes to compute the matrix's norm */
public class MatrixNormMapRed {

  /**
   * Initialize the job to compute the matrix's norm
   * 
   * @param inputMatrixPath the input matrix's path
   * @param outputPath the output file's name that records the norm of the
   *                matrix
   * @param mapper Mapper
   * @param combiner Combiner
   * @param reducer Reducer
   * @param jobConf Configuration of the job
   */
  public static void initJob(String inputMatrixPath, String outputPath,
      Class<? extends MatrixNormMapper> mapper,
      Class<? extends MatrixNormReducer> combiner,
      Class<? extends MatrixNormReducer> reducer, JobConf jobConf) {
    jobConf.setMapperClass(mapper);
    jobConf.setMapOutputKeyClass(IntWritable.class);
    jobConf.setMapOutputValueClass(DoubleWritable.class);
    jobConf.setCombinerClass(combiner);
    jobConf.setReducerClass(reducer);
    jobConf.setOutputKeyClass(IntWritable.class);
    jobConf.setOutputValueClass(DoubleWritable.class);

    // input
    jobConf.setInputFormat(VectorInputFormat.class);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
    FileInputFormat.addInputPaths(jobConf, inputMatrixPath);
    // output
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
  }

  /** the interface of norm mapper */
  public static interface MatrixNormMapper extends
      Mapper<IntWritable, MapWritable, IntWritable, DoubleWritable> {
    IntWritable nKey = new IntWritable(-1);
    DoubleWritable nValue = new DoubleWritable(0);
  }

  /** the interface of norm reducer/combiner */
  public static interface MatrixNormReducer extends
      Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    IntWritable nKey = new IntWritable(-1);
    DoubleWritable nValue = new DoubleWritable(0);
  }

  // /
  // / Infinity Norm
  // /

  /** Infinity Norm */
  public static class MatrixInfinityNormMapper extends MapReduceBase implements
      MatrixNormMapper {

    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {

      double rowSum = 0;
      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        rowSum += Math.abs(((DoubleEntry) e.getValue()).getValue());
      }
      nValue.set(rowSum);

      output.collect(nKey, nValue);
    }

  }

  /**
   * Matrix Infinity Norm Reducer
   */
  public static class MatrixInfinityNormReducer extends MapReduceBase implements
      MatrixNormReducer {

    private double max = 0;

    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {

      while (values.hasNext()) {
        max = Math.max(values.next().get(), max);
      }

      // Note: Tricky here. As we known, we collect each row's sum with key(-1).
      // the reduce will just iterate through one key (-1)
      // so we collect the max sum-value here
      nValue.set(max);
      output.collect(nKey, nValue);
    }

  }

  // /
  // / One Norm
  // /

  /** One Norm Mapper */
  public static class MatrixOneNormMapper extends MapReduceBase implements
      MatrixNormMapper {

    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {

      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        nValue.set(((DoubleEntry) e.getValue()).getValue());
        output.collect((IntWritable) e.getKey(), nValue);
      }
    }
  }

  /** One Norm Combiner * */
  public static class MatrixOneNormCombiner extends MapReduceBase implements
      MatrixNormReducer {

    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {

      double partialColSum = 0;
      while (values.hasNext()) {
        partialColSum += values.next().get();
      }
      nValue.set(partialColSum);
      output.collect(key, nValue);
    }
  }

  /** One Norm Reducer * */
  public static class MatrixOneNormReducer extends MapReduceBase implements
      MatrixNormReducer {
    private double max = 0;
    private Path outDir;
    private JobConf conf;

    @Override
    public void configure(JobConf job) {
      outDir = FileOutputFormat.getOutputPath(job);
      conf = job;
    }

    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {
      double colSum = 0;
      while (values.hasNext()) {
        colSum += values.next().get();
      }

      max = Math.max(Math.abs(colSum), max);
    }

    @Override
    public void close() throws IOException {
      // write output to a file
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
          outFile, IntWritable.class, DoubleWritable.class,
          CompressionType.NONE);
      writer.append(new IntWritable(-1), new DoubleWritable(max));
      writer.close();
    }
  }

  // /
  // / Frobenius Norm
  // /

  /** Frobenius Norm Mapper */
  public static class MatrixFrobeniusNormMapper extends MapReduceBase implements
      MatrixNormMapper {
    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {
      double rowSqrtSum = 0;
      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        double cellValue = ((DoubleEntry) e.getValue()).getValue();
        rowSqrtSum += (cellValue * cellValue);
      }

      nValue.set(rowSqrtSum);
      output.collect(nKey, nValue);
    }
  }

  /** Frobenius Norm Combiner */
  public static class MatrixFrobeniusNormCombiner extends MapReduceBase
      implements MatrixNormReducer {
    private double sqrtSum = 0;

    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        sqrtSum += values.next().get();
      }
      // Note: Tricky here. As we known, we collect each row's sum with key(-1).
      // the reduce will just iterate through one key (-1)
      // so we collect the max sum-value here
      nValue.set(sqrtSum);
      output.collect(nKey, nValue);
    }
  }

  /** Frobenius Norm Reducer */
  public static class MatrixFrobeniusNormReducer extends MapReduceBase
      implements MatrixNormReducer {
    private double sqrtSum = 0;

    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        sqrtSum += values.next().get();
      }

      // Note: Tricky here. As we known, we collect each row's sum with key(-1).
      // the reduce will just iterate through one key (-1)
      // so we collect the max sum-value here
      nValue.set(Math.sqrt(sqrtSum));
      output.collect(nKey, nValue);
    }
  }

  // /
  // / MaxValue Norm
  // /

  /** MaxValue Norm Mapper * */
  public static class MatrixMaxValueNormMapper extends MapReduceBase implements
      MatrixNormMapper {
    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {
      double max = 0;
      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        double cellValue = Math.abs(((DoubleEntry) e.getValue()).getValue());
        max = cellValue > max ? cellValue : max;
      }

      nValue.set(max);
      output.collect(nKey, nValue);
    }

  }

  /** MaxValue Norm Reducer */
  public static class MatrixMaxValueNormReducer extends MapReduceBase implements
      MatrixNormReducer {
    private double max = 0;

    @Override
    public void reduce(IntWritable key, Iterator<DoubleWritable> values,
        OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        max = Math.max(values.next().get(), max);
      }

      // Note: Tricky here. As we known, we collect each row's sum with key(-1).
      // the reduce will just iterate through one key (-1)
      // so we collect the max sum-value here
      nValue.set(max);
      output.collect(nKey, nValue);
    }
  }
}
