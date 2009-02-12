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

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.Matrix;
import org.apache.hama.algebra.BlockMultiplyMap;
import org.apache.hama.algebra.BlockMultiplyReduce;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.mapred.CollectBlocksMap;
import org.apache.hama.mapred.CollectBlocksMapReduceBase;
import org.apache.hama.util.JobManager;

public class TestFileMatrixBlockMult extends TestCase {
  final static Log LOG = LogFactory.getLog(TestFileMatrixBlockMult.class
      .getName());
  private static HamaConfiguration conf;
  private static Path[] path = new Path[2];
  private static Matrix collectionTable;

  public static Test suite() {
    TestSetup setup = new TestSetup(
        new TestSuite(TestFileMatrixBlockMult.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        conf = hCluster.getConf();
        collectionTable = new DenseMatrix(conf);
      }

      protected void tearDown() {
      }
    };
    return setup;
  }

  public void testCreateFiles() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = new LocalFileSystem();
    fs.setConf(conf);
    fs.getRawFileSystem().setConf(conf);

    for (int i = 0; i < 2; i++) {
      path[i] = new Path(System.getProperty("test.build.data", ".") + "/test"
          + i + ".seq");
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path[i],
          IntWritable.class, MapWritable.class, CompressionType.BLOCK);

      MapWritable value = new MapWritable();
      value.put(new IntWritable(0), new DoubleWritable(0.5));
      value.put(new IntWritable(1), new DoubleWritable(0.1));
      value.put(new IntWritable(2), new DoubleWritable(0.5));
      value.put(new IntWritable(3), new DoubleWritable(0.1));

      writer.append(new IntWritable(0), value);
      writer.append(new IntWritable(1), value);
      writer.append(new IntWritable(2), value);
      writer.append(new IntWritable(3), value);

      writer.close();
    }

    SequenceFile.Reader reader1 = new SequenceFile.Reader(fs, path[0], conf);
    // read first value from reader1
    IntWritable key = new IntWritable();
    MapWritable val = new MapWritable();
    reader1.next(key, val);

    assertEquals(key.get(), 0);
  }

  public void testFileMatrixMult() throws IOException {
    collectBlocksFromFile(path[0], true, collectionTable.getPath(), conf);
    collectBlocksFromFile(path[1], false, collectionTable.getPath(), conf);

    Matrix result = new DenseMatrix(conf);
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    BlockMultiplyMap.initJob(collectionTable.getPath(), BlockMultiplyMap.class,
        BlockID.class, BlockWritable.class, jobConf);
    BlockMultiplyReduce.initJob(result.getPath(), BlockMultiplyReduce.class,
        jobConf);

    JobManager.execute(jobConf, result);

    verifyMultResult(result);
  }

  private void verifyMultResult(Matrix result) throws IOException {
    double[][] a = new double[][] { { 0.5, 0.1, 0.5, 0.1 },
        { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 } };
    double[][] b = new double[][] { { 0.5, 0.1, 0.5, 0.1 },
        { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 } };
    double[][] c = new double[4][4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; j++) {
        for (int k = 0; k < 4; k++) {
          c[i][k] += a[i][j] * b[j][k];
        }
      }
    }

    for (int i = 0; i < result.getRows(); i++) {
      for (int j = 0; j < result.getColumns(); j++) {
        double gap = (c[i][j] - result.get(i, j));
        assertTrue(gap < 0.000001 || gap < -0.000001);
      }
    }    
  }

  private static void collectBlocksFromFile(Path path, boolean b,
      String collectionTable, HamaConfiguration conf) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("Blocking MR job" + path);

    jobConf.setMapperClass(MyMapper.class);
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(jobConf, path);

    MyMapper.initJob(collectionTable, b, 2, 4, 4, jobConf);
    JobClient.runJob(jobConf);
  }

  public static class MyMapper extends CollectBlocksMapReduceBase implements
      CollectBlocksMap<IntWritable, MapWritable> {
    private MapWritable value;

    @Override
    public void map(IntWritable key, MapWritable value,
        OutputCollector<BlockID, VectorWritable> output, Reporter reporter)
        throws IOException {
      int startColumn;
      int endColumn;
      int blkRow = key.get() / mBlockRowSize;
      this.value = value;

      int i = 0;
      do {
        startColumn = i * mBlockColSize;
        endColumn = startColumn + mBlockColSize - 1;
        if (endColumn >= mColumns) // the last sub vector
          endColumn = mColumns - 1;
        output.collect(new BlockID(blkRow, i), new VectorWritable(key.get(),
            subVector(startColumn, endColumn)));

        i++;
      } while (endColumn < (mColumns - 1));
    }

    private DenseVector subVector(int i0, int i1) {
      DenseVector res = new DenseVector();
      for (int i = i0; i <= i1; i++) {
        res.set(i, ((DoubleWritable) this.value.get(new IntWritable(i))).get());
      }

      return res;
    }
  }
}
