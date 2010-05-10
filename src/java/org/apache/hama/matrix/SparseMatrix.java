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
package org.apache.hama.matrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.algebra.SparseMatrixVectorMultMap;
import org.apache.hama.matrix.algebra.SparseMatrixVectorMultReduce;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.RandomVariable;

public class SparseMatrix extends AbstractMatrix implements Matrix {
  static private final String TABLE_PREFIX = SparseMatrix.class.getSimpleName();

  public SparseMatrix(HamaConfiguration conf, int m, int n) throws IOException {
    setConfiguration(conf);

    tryToCreateTable(TABLE_PREFIX);
    closed = false;
    this.setDimension(m, n);
  }

  /**
   * Load a matrix from an existed matrix table whose tablename is 'matrixpath' !!
   * It is an internal used for map/reduce.
   * 
   * @param conf configuration object
   * @param matrixpath
   * @throws IOException
   * @throws IOException
   */
  public SparseMatrix(HamaConfiguration conf, String matrixpath)
      throws IOException {
    setConfiguration(conf);
    matrixPath = matrixpath;
    // load the matrix
    table = new HTable(conf, matrixPath);
    // TODO: now we don't increment the reference of the table
    // for it's an internal use for map/reduce.
    // if we want to increment the reference of the table,
    // we don't know where to call Matrix.close in Add & Mul map/reduce
    // process to decrement the reference. It seems difficulty.
  }

  /**
   * Generate matrix with random elements
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @return an m-by-n matrix with uniformly distributed random elements.
   * @throws IOException
   */
  public static SparseMatrix random(HamaConfiguration conf, int m, int n)
      throws IOException {
    SparseMatrix rand = new SparseMatrix(conf, m, n);
    SparseVector vector = new SparseVector();
    LOG.info("Create the " + m + " * " + n + " random matrix : "
        + rand.getPath());

    for (int i = 0; i < m; i++) {
      vector.clear();
      for (int j = 0; j < n; j++) {
        Random r = new Random();
        if (r.nextInt(2) != 0)
          vector.set(j, RandomVariable.rand());
      }
      rand.setRow(i, vector);
    }

    return rand;
  }

  @Override
  public Matrix add(Matrix B) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Matrix add(double alpha, Matrix B) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double get(int i, int j) throws IOException {
    if (this.getRows() < i || this.getColumns() < j)
      throw new ArrayIndexOutOfBoundsException(i + ", " + j);

    Get get = new Get(BytesUtil.getRowIndex(i));
    get.addColumn(Constants.COLUMNFAMILY);
    byte[] result = table.get(get).getValue(Constants.COLUMNFAMILY,
        Bytes.toBytes(String.valueOf(j)));
    return (result != null) ? Bytes.toDouble(result) : 0.0;
  }

  @Override
  public Vector getColumn(int j) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Gets the vector of row
   * 
   * @param i the row index of the matrix
   * @return the vector of row
   * @throws IOException
   */
  public SparseVector getRow(int i) throws IOException {
    Get get = new Get(BytesUtil.getRowIndex(i));
    get.addFamily(Constants.COLUMNFAMILY);
    Result r = table.get(get);
    // return new SparseVector(r.getRowResult());
    return new SparseVector(r);
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double value) throws IOException {
    if (value != 0) {
      Put put = new Put(BytesUtil.getRowIndex(i));
      put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j)),
          Bytes.toBytes(value));
      table.put(put);
    }
  }

  /**
   * Returns type of matrix
   */
  public String getType() {
    return this.getClass().getSimpleName();
  }

  /**
   * C = A*B using iterative method
   * 
   * @param B
   * @return C
   * @throws IOException
   */
  public SparseMatrix mult(Matrix B) throws IOException {
    SparseMatrix result = new SparseMatrix(config, this.getRows(), this
        .getColumns());

    List<Job> jobId = new ArrayList<Job>();

    for (int i = 0; i < this.getRows(); i++) {
      Job job = new Job(config, "multiplication MR job : " + result.getPath()
          + " " + i);

      Scan scan = new Scan();
      scan.addFamily(Constants.COLUMNFAMILY);
      job.getConfiguration().set(SparseMatrixVectorMultMap.MATRIX_A,
          this.getPath());
      job.getConfiguration().setInt(SparseMatrixVectorMultMap.ITH_ROW, i);

      TableMapReduceUtil.initTableMapperJob(B.getPath(), scan,
          SparseMatrixVectorMultMap.class, IntWritable.class,
          MapWritable.class, job);
      TableMapReduceUtil.initTableReducerJob(result.getPath(),
          SparseMatrixVectorMultReduce.class, job);
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

  @Override
  public Matrix multAdd(double alpha, Matrix B, Matrix C) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setColumn(int column, Vector vector) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRow(int row, Vector vector) throws IOException {
    if (this.getRows() < row)
      throw new ArrayIndexOutOfBoundsException(row);

    if (vector.size() > 0) { // stores if size > 0
      Put put = new Put(BytesUtil.getRowIndex(row));
      for (Map.Entry<Writable, Writable> e : ((SparseVector) vector).getEntries().entrySet()) {
        put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(((IntWritable) e.getKey()).get())),
            Bytes.toBytes(((DoubleWritable) e.getValue()).get()));
      }
      table.put(put);
    }
  }

  @Override
  public SubMatrix subMatrix(int i0, int i1, int j0, int j1) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
