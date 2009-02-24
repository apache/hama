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
package org.apache.hama;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.algebra.SIMDMultiplyMap;
import org.apache.hama.algebra.SIMDMultiplyReduce;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.JobManager;
import org.apache.hama.util.RandomVariable;

public class SparseMatrix extends AbstractMatrix implements Matrix {

  public SparseMatrix(HamaConfiguration conf) throws IOException {
    setConfiguration(conf);

    tryToCreateTable();

    closed = false;
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
    SparseMatrix rand = new SparseMatrix(conf);
    SparseVector vector = new SparseVector();
    LOG.info("Create the " + m + " * " + n + " random matrix : "
        + rand.getPath());

    for (int i = 0; i < m; i++) {
      vector.clear();
      for (int j = 0; j < n; j++) {
        Random r = new Random(); 
        if(r.nextInt(2) != 0)
          vector.set(j, RandomVariable.rand());
      }
      rand.setRow(i, vector);
    }

    rand.setDimension(m, n);
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
    if(this.getRows() < i || this.getColumns() < j)
      throw new ArrayIndexOutOfBoundsException(i +", "+ j);
    
    Cell c = table.get(BytesUtil.getRowIndex(i), BytesUtil.getColumnIndex(j));
    return (c != null) ? BytesUtil.bytesToDouble(c.getValue()) : 0.0;
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
    // Should returns zero-fill vector.
    return new SparseVector(table.getRow(BytesUtil.getRowIndex(i)));
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double value) throws IOException {
    if(value != 0) {
      VectorUpdate update = new VectorUpdate(i);
      update.put(j, value);
      table.commit(update.getBatchUpdate());
    }
  }
  
  /**
   * Returns type of matrix
   */
  public String getType() {
    return this.getClass().getSimpleName();
  }

  @Override
  public SparseMatrix mult(Matrix B) throws IOException {
    SparseMatrix result = new SparseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    SIMDMultiplyMap.initJob(this.getPath(), B.getPath(), this.getType(), SIMDMultiplyMap.class,
        IntWritable.class, MapWritable.class, jobConf);
    SIMDMultiplyReduce.initJob(result.getPath(), SIMDMultiplyReduce.class,
        jobConf);
    JobManager.execute(jobConf);
    result.setDimension(this.getRows(), this.getColumns());
    return result;
  }

  @Override
  public Matrix multAdd(double alpha, Matrix B, Matrix C) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double norm(Norm type) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setColumn(int column, Vector vector) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setRow(int row, Vector vector) throws IOException {
    if(this.getRows() < row)
      increaseRows();
    
    if(vector.size() > 0) {  // stores if size > 0
      VectorUpdate update = new VectorUpdate(row);
      update.putAll(((SparseVector) vector).getEntries());
      table.commit(update.getBatchUpdate());
    }
  }

  @Override
  public SubMatrix subMatrix(int i0, int i1, int j0, int j1) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
