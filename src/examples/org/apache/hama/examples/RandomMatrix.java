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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.mapreduce.RandomMatrixMapper;
import org.apache.hama.examples.mapreduce.RandomMatrixReducer;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.matrix.SparseMatrix;

public class RandomMatrix extends AbstractExample {
  static private String TABLE_PREFIX;
  static private Path TMP_DIR;
  
  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.out
          .println("rand [-m maps] [-r reduces] <rows> <columns> <density> <matrix_name>");
      System.out
      .println("ex) rand -m 10 -r 10 2000 2000 30.5% matrixA");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    int row = Integer.parseInt(ARGS.get(0));
    int column = Integer.parseInt(ARGS.get(1));
    double percent = Double.parseDouble(ARGS.get(2).substring(0, ARGS.get(2).length()-1));
    
    Matrix a;
    if(percent == 100)
      a = random_mapred(conf, row, column);
    else
      a = random_mapred(conf, row, column, percent);
    
    a.save(ARGS.get(3));
  }
  

  /**
   * Generate matrix with random elements using Map/Reduce
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @return an m-by-n matrix with uniformly distributed random elements.
   * @throws IOException
   */
  public static DenseMatrix random_mapred(HamaConfiguration conf, int m, int n)
      throws IOException {
	  TABLE_PREFIX = "DenseMatrix";
	  TMP_DIR = new Path(TABLE_PREFIX + "_TMP_dir");
    DenseMatrix rand = new DenseMatrix(conf, m, n);

    Job job = new Job(conf, "random matrix MR job : " + rand.getPath());
    final Path inDir = new Path(TMP_DIR, "in");
    FileInputFormat.setInputPaths(job, inDir);
    job.setMapperClass(RandomMatrixMapper.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(MapWritable.class);

    job.getConfiguration().setInt("matrix.column", n);
    job.getConfiguration().set("matrix.type", TABLE_PREFIX);
    job.getConfiguration().set("matrix.density", "100");

    job.setInputFormatClass(SequenceFileInputFormat.class);
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    int interval = m / conf.getNumMapTasks();

    // generate an input file for each map task
    for (int i = 0; i < conf.getNumMapTasks(); ++i) {
      final Path file = new Path(inDir, "part" + i);
      final IntWritable start = new IntWritable(i * interval);
      IntWritable end = null;
      if ((i + 1) != conf.getNumMapTasks()) {
        end = new IntWritable(((i * interval) + interval) - 1);
      } else {
        end = new IntWritable(m - 1);
      }
      final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
          .getConfiguration(), file, IntWritable.class, IntWritable.class,
          CompressionType.NONE);
      try {
        writer.append(start, end);
      } finally {
        writer.close();
      }
      System.out.println("Wrote input for Map #" + i);
    }

    job.setOutputFormatClass(TableOutputFormat.class);
    job.setReducerClass(RandomMatrixReducer.class);
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, rand.getPath());
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    fs.delete(TMP_DIR, true);
    return rand;
  }

  public static SparseMatrix random_mapred(HamaConfiguration conf, int m,
	      int n, double percent) throws IOException {
     TABLE_PREFIX = "SparseMatrix";
     TMP_DIR = new Path(TABLE_PREFIX + "_TMP_dir");
     SparseMatrix rand = new SparseMatrix(conf, m, n);

	    Job job = new Job(conf, "random matrix MR job : " + rand.getPath());
	    final Path inDir = new Path(TMP_DIR, "in");
	    FileInputFormat.setInputPaths(job, inDir);
	    job.setMapperClass(RandomMatrixMapper.class);

	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(MapWritable.class);

	    job.getConfiguration().setInt("matrix.column", n);
	    job.getConfiguration().set("matrix.type", TABLE_PREFIX);
	    job.getConfiguration().set("matrix.density", String.valueOf(percent));

	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    final FileSystem fs = FileSystem.get(job.getConfiguration());
	    int interval = m / conf.getNumMapTasks();

	    // generate an input file for each map task
	    for (int i = 0; i < conf.getNumMapTasks(); ++i) {
	      final Path file = new Path(inDir, "part" + i);
	      final IntWritable start = new IntWritable(i * interval);
	      IntWritable end = null;
	      if ((i + 1) != conf.getNumMapTasks()) {
	        end = new IntWritable(((i * interval) + interval) - 1);
	      } else {
	        end = new IntWritable(m - 1);
	      }
	      final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
	          .getConfiguration(), file, IntWritable.class, IntWritable.class,
	          CompressionType.NONE);
	      try {
	        writer.append(start, end);
	      } finally {
	        writer.close();
	      }
	      System.out.println("Wrote input for Map #" + i);
	    }

	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.setReducerClass(RandomMatrixReducer.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, rand.getPath());
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Writable.class);

	    try {
	      job.waitForCompletion(true);
	    } catch (InterruptedException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    }
	    fs.delete(TMP_DIR, true);
	    return rand;
	  }

}
