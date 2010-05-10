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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hama.Constants;
import org.apache.hama.HamaAdmin;
import org.apache.hama.HamaAdminImpl;
import org.apache.hama.examples.mapreduce.*;
import org.apache.hama.io.BlockID;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.matrix.SparseMatrix;
import org.apache.hama.util.RandomVariable;

public class MatrixMultiplication extends AbstractExample {
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out
          .println("mult [-m maps] [-r reduces] <matrix A> <matrix B> [blocks]");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    String matrixA = ARGS.get(0);
    String matrixB = ARGS.get(1);

    HamaAdmin admin = new HamaAdminImpl(conf);
    Matrix a = admin.getMatrix(matrixA);
    Matrix b = admin.getMatrix(matrixB);

    if (!a.getType().equals(b.getType())) {
      System.out.println(a.getType() + " != " + b.getType());
      System.exit(-1);
    }

    Matrix c;
    if (a.getType().equals("SparseMatrix")) {
      if (ARGS.size() > 2) {
        System.out
            .println("NOTE: You can't use the block algorithm for sparse matrix multiplication.");
      }
      c = ((SparseMatrix) a).mult(b);
    } else {
      if (ARGS.size() > 2) {
        c = mult(a, b, Integer.parseInt(ARGS.get(2)));
      } else {
        c = mult(a, b);
      }
    }

    for (int i = 0; i < 2; i++) {
      System.out.println("c(" + 0 + ", " + i + ") : " + c.get(0, i));
    }
    System.out.println("...");
  }

  /**
   * C = A*B using iterative method
   * 
   * @param B
   * @return C
   * @throws IOException
   */
  public static DenseMatrix mult(Matrix A, Matrix B) throws IOException {
    ensureForMultiplication(A, B);
    int columns = 0;
    if (B.getColumns() == 1 || A.getColumns() == 1)
      columns = 1;
    else
      columns = A.getColumns();

    DenseMatrix result = new DenseMatrix(conf, A.getRows(), columns);
    List<Job> jobId = new ArrayList<Job>();

    for (int i = 0; i < A.getRows(); i++) {
      Job job = new Job(conf, "multiplication MR job : " + result.getPath()
          + " " + i);

      Scan scan = new Scan();
      scan.addFamily(Constants.COLUMNFAMILY);
      job.getConfiguration()
          .set(DenseMatrixVectorMultMap.MATRIX_A, A.getPath());
      job.getConfiguration().setInt(DenseMatrixVectorMultMap.ITH_ROW, i);

      TableMapReduceUtil.initTableMapperJob(B.getPath(), scan,
          DenseMatrixVectorMultMap.class, IntWritable.class, MapWritable.class,
          job);
      TableMapReduceUtil.initTableReducerJob(result.getPath(),
          DenseMatrixVectorMultReduce.class, job);
      try {
        job.waitForCompletion(false);
        jobId.add(job);
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }

    while (checkAllJobs(jobId) == false) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    return result;
  }

  /**
   * C = A * B using Blocking algorithm
   * 
   * @param B
   * @param blocks the number of blocks
   * @return C
   * @throws IOException
   */
  public static DenseMatrix mult(Matrix A, Matrix B, int blocks)
      throws IOException {
    ensureForMultiplication(A, B);
    
    String collectionTable = "collect_" + RandomVariable.randMatrixPath();
    HTableDescriptor desc = new HTableDescriptor(collectionTable);
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes(Constants.BLOCK)));
    new HBaseAdmin(conf).createTable(desc);

    collectBlocksMapRed(A, collectionTable, blocks, true);
    collectBlocksMapRed(B, collectionTable, blocks, false);

    DenseMatrix result = new DenseMatrix(conf, A.getRows(), A.getColumns());

    Job job = new Job(conf, "multiplication MR job : " + result.getPath());

    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(Constants.BLOCK));

    TableMapReduceUtil.initTableMapperJob(collectionTable, scan,
        BlockMultMap.class, BlockID.class, BytesWritable.class, job);
    TableMapReduceUtil.initTableReducerJob(result.getPath(),
        BlockMultReduce.class, job);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    new HamaAdminImpl(conf, new HBaseAdmin(conf)).delete(collectionTable);
    return result;
  }

  /**
   * Collect Blocks
   * 
   * @param path a input path
   * @param collectionTable the collection table
   * @param blockNum the number of blocks
   * @param bool
   * @throws IOException
   */
  public static void collectBlocksMapRed(Matrix m, String collectionTable,
      int blockNum, boolean bool) throws IOException {
    double blocks = Math.pow(blockNum, 0.5);
    if (!String.valueOf(blocks).endsWith(".0"))
      throw new IOException("can't divide.");

    int block_size = (int) blocks;
    Job job = new Job(conf, "Blocking MR job" + m.getPath());

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    job.getConfiguration().set(CollectBlocksMapper.BLOCK_SIZE,
        String.valueOf(block_size));
    job.getConfiguration().set(CollectBlocksMapper.ROWS,
        String.valueOf(m.getRows()));
    job.getConfiguration().set(CollectBlocksMapper.COLUMNS,
        String.valueOf(m.getColumns()));
    job.getConfiguration().setBoolean(CollectBlocksMapper.MATRIX_POS, bool);

    TableMapReduceUtil.initTableMapperJob(m.getPath(), scan,
        org.apache.hama.examples.mapreduce.CollectBlocksMapper.class, BlockID.class,
        MapWritable.class, job);
    TableMapReduceUtil.initTableReducerJob(collectionTable,
        org.apache.hama.examples.mapreduce.CollectBlocksReducer.class, job);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static void ensureForMultiplication(Matrix A, Matrix m)
      throws IOException {
    if (A.getColumns() != m.getRows()) {
      throw new IOException("A's columns should equal with B's rows while A*B.");
    }
  }

  public static boolean checkAllJobs(List<Job> jobId) throws IOException {
    Iterator<Job> it = jobId.iterator();
    boolean allTrue = true;
    while (it.hasNext()) {
      if (!it.next().isComplete()) {
        allTrue = false;
      }
    }

    return allTrue;
  }
}
