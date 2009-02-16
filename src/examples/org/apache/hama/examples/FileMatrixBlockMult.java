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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.algebra.BlockMultiplyMap;
import org.apache.hama.algebra.BlockMultiplyReduce;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.mapred.CollectBlocksMap;
import org.apache.hama.mapred.CollectBlocksMapReduceBase;
import org.apache.hama.util.JobManager;

public class FileMatrixBlockMult extends AbstractExample {
  final static Log LOG = LogFactory.getLog(FileMatrixBlockMult.class.getName());
  private static int BLOCKSIZE;
  private static int ROWS;
  private static int COLUMNS;

  /**
   * Collect blocks from sequence file,
   */
  public static class MyMapper extends CollectBlocksMapReduceBase implements
      CollectBlocksMap<IntWritable, MapWritable> {
    private MapWritable value;

    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<BlockID, MapWritable> output, Reporter reporter)
        throws IOException {
      int startColumn, endColumn, blkRow = key.get() / mBlockRowSize, i = 0;
      this.value = value;
      
      do {
        startColumn = i * mBlockColSize;
        endColumn = startColumn + mBlockColSize - 1;
        if (endColumn >= mColumns) // the last sub vector
          endColumn = mColumns - 1;
        output.collect(new BlockID(blkRow, i), subVector(key.get(), startColumn, endColumn));

        i++;
      } while (endColumn < (mColumns - 1));
    }

    private MapWritable subVector(int row, int i0, int i1) {
      DenseVector res = new DenseVector();
      res.setRow(row);
      
      for (int i = i0; i <= i1; i++) {
        res.set(i, ((DoubleWritable) this.value.get(new IntWritable(i))).get());
      }

      return res.getEntries();
    }
  }

  /**
   * @param a the path of matrix A
   * @param b the path of matrix B
   * @return the result C
   * @throws IOException
   */
  private static DenseMatrix matMult(Path a, Path b) throws IOException {
    HamaConfiguration conf = new HamaConfiguration();
    Matrix collectionTable = new DenseMatrix(conf);

    collectBlocksFromFile(a, true, collectionTable.getPath(), conf);
    collectBlocksFromFile(b, false, collectionTable.getPath(), conf);

    DenseMatrix result = new DenseMatrix(conf);
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    BlockMultiplyMap.initJob(collectionTable.getPath(), BlockMultiplyMap.class,
        BlockID.class, BlockWritable.class, jobConf);
    BlockMultiplyReduce.initJob(result.getPath(), BlockMultiplyReduce.class,
        jobConf);

    JobManager.execute(jobConf, result);

    return result;
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 5) {
      System.out
          .println("multfiles  [-m maps] [-r reduces] <seqfile1> <seqfile1> <blocks> <rows> <columns>");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    Path a = new Path(ARGS.get(0));
    Path b = new Path(ARGS.get(1));

    BLOCKSIZE = Integer.parseInt(ARGS.get(2));
    // You should know dimensions
    ROWS = Integer.parseInt(ARGS.get(3));
    COLUMNS = Integer.parseInt(ARGS.get(4));

    DenseMatrix result = matMult(a, b);
    System.out.println("result: " + result.getRows() + " by "
        + result.getColumns());
  }

  private static void collectBlocksFromFile(Path path, boolean b,
      String collectionTable, HamaConfiguration conf) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("Blocking MR job" + path);

    jobConf.setMapperClass(MyMapper.class);
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(jobConf, path);

    MyMapper.initJob(collectionTable, b, BLOCKSIZE, ROWS, COLUMNS, jobConf);
    JobClient.runJob(jobConf);
  }
}
