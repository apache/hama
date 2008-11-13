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
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.algebra.RowCyclicAdditionMap;
import org.apache.hama.algebra.RowCyclicAdditionReduce;
import org.apache.hama.algebra.SIMDMultiplyMap;
import org.apache.hama.algebra.SIMDMultiplyReduce;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.io.VectorMapWritable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.mapred.RowCyclicReduce;
import org.apache.hama.util.JobManager;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.RandomVariable;

public class DenseMatrix extends AbstractMatrix implements Matrix {

  static int tryPathLength = Constants.DEFAULT_PATH_LENGTH;
  static final String TABLE_PREFIX = DenseMatrix.class.getSimpleName() + "_";
  
  /**
   * Construct a raw matrix.
   * Just create a table in HBase, but didn't lay any schema ( such as
   * dimensions: i, j ) on it.
   * 
   * @param conf configuration object
   * @throws IOException 
   *         throw the exception to let the user know what happend, if we
   *         didn't create the matrix successfully.
   */
  public DenseMatrix(HamaConfiguration conf) throws IOException {
    setConfiguration(conf);
    
    tryToCreateTable();
    
    closed = false;
  }

  /**
   * Create/load a matrix aliased as 'matrixName'.
   * 
   * @param conf configuration object
   * @param matrixName the name of the matrix
   * @param force if force is true, a new matrix will be created 
   *              no matter 'matrixName' has aliased to an existed matrix;
   *              otherwise, just try to load an existed matrix alised 
   *              'matrixName'. 
   * @throws IOException 
   */
  public DenseMatrix(HamaConfiguration conf, String matrixName, 
      boolean force) throws IOException {
    setConfiguration(conf);
    // if force is set to true:
    // 1) if this matrixName has aliase to other matrix, we will remove 
    //    the old aliase, create a new matrix table, and aliase to it.
    // 2) if this matrixName has no aliase to other matrix, we will create
    //    a new matrix table, and alise to it.
    //
    // if force is set to false, we just try to load an existed matrix alised
    // as 'matrixname'.
    
    boolean existed = hamaAdmin.matrixExists(matrixName);
    
    if (force) {
      if(existed) {
        // remove the old aliase
        hamaAdmin.delete(matrixName);
      }
      // create a new matrix table.
      tryToCreateTable();
      // save the new aliase relationship
      save(matrixName);
    } else {
      if(existed) {
        // try to get the actual path of the table
        matrixPath = hamaAdmin.getPath(matrixName);
        // load the matrix
        table = new HTable(conf, matrixPath);
        // increment the reference
        incrementAndGetRef();
      } else {
        throw new IOException("Try to load non-existed matrix alised as " + matrixName);
      }
    }

    closed = false;
  }
  
  /**
   * Load a matrix from an existed matrix table whose tablename is 'matrixpath'
   * 
   * !! It is an internal used for map/reduce.
   * 
   * @param conf configuration object
   * @param matrixpath 
   * @throws IOException 
   * @throws IOException 
   */
  public DenseMatrix(HamaConfiguration conf, String matrixpath) throws IOException {
    setConfiguration(conf);
    matrixPath = matrixpath;
    // load the matrix
    table = new HTable(conf, matrixPath);
    // TODO: now we don't increment the reference of the table
    //       for it's an internal use for map/reduce.
    //       if we want to increment the reference of the table,
    //       we don't know where to call Matrix.close in Add & Mul map/reduce
    //       process to decrement the reference. It seems difficulty.
  }

  /**
   * Create an m-by-n constant matrix.
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @param s fill the matrix with this scalar value.
   * @throws IOException 
   *         throw the exception to let the user know what happend, if we
   *         didn't create the matrix successfully.
   */
  public DenseMatrix(HamaConfiguration conf, int m, int n, double s) throws IOException {
    setConfiguration(conf);
    
    tryToCreateTable();

    closed = false;
    
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        set(i, j, s);
      }
    }

    setDimension(m, n);
  }
  
  /** try to create a new matrix with a new random name.
   *  try times will be (Integer.MAX_VALUE - 4) * DEFAULT_TRY_TIMES;
   * @throws IOException
   */
  private void tryToCreateTable() throws IOException {
    int tryTimes = Constants.DEFAULT_TRY_TIMES;
    do {
      matrixPath = TABLE_PREFIX + RandomVariable.randMatrixPath(tryPathLength);
      
      if (!admin.tableExists(matrixPath)) { // no table 'matrixPath' in hbase.
        tableDesc = new HTableDescriptor(matrixPath);
        create();
        return;
      }
      
      tryTimes--;
      if(tryTimes <= 0) { // this loop has exhausted DEFAULT_TRY_TIMES.
        tryPathLength++;
        tryTimes = Constants.DEFAULT_TRY_TIMES;
      }
      
    } while(tryPathLength <= Constants.DEFAULT_MAXPATHLEN);
    // exhaustes the try times.
    // throw out an IOException to let the user know what happened.
    throw new IOException("Try too many times to create a table in hbase.");
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
  public static Matrix random(HamaConfiguration conf, int m, int n)
      throws IOException {
    Matrix rand = new DenseMatrix(conf);
    DenseVector vector = new DenseVector();
    LOG.info("Create the " + m + " * " + n + " random matrix : " + rand.getPath());

    for (int i = 0; i < m; i++) {
      vector.clear();
      for (int j = 0; j < n; j++) {
        vector.set(j, RandomVariable.rand());
      }
      rand.setRow(i, vector);
    }

    rand.setDimension(m, n);
    return rand;
  }

  /**
   * Generate identity matrix
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @return an m-by-n matrix with ones on the diagonal and zeros elsewhere.
   * @throws IOException
   */
  public static Matrix identity(HamaConfiguration conf, int m, int n)
      throws IOException {
    Matrix identity = new DenseMatrix(conf);
    LOG.info("Create the " + m + " * " + n + " identity matrix : " + identity.getPath());
    
    for (int i = 0; i < m; i++) {
      DenseVector vector = new DenseVector();
      for (int j = 0; j < n; j++) {
        vector.set(j, (i == j ? 1.0 : 0.0));
      }
      identity.setRow(i, vector);
    }

    identity.setDimension(m, n);
    return identity;
  }

  public Matrix add(Matrix B) throws IOException {
    Matrix result = new DenseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job" + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    RowCyclicAdditionMap.initJob(this.getPath(), B.getPath(), RowCyclicAdditionMap.class,
        IntWritable.class, VectorWritable.class, jobConf);
    RowCyclicReduce.initJob(result.getPath(), RowCyclicAdditionReduce.class, jobConf);

    JobManager.execute(jobConf, result);
    return result;
  }

  public Matrix add(double alpha, Matrix B) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public DenseVector getRow(int row) throws IOException {
    return new DenseVector(table.getRow(BytesUtil.intToBytes(row)));
  }

  public Vector getColumn(int column) throws IOException {
    byte[] columnKey = BytesUtil.getColumnIndex(column);
    byte[][] c = { columnKey };
    Scanner scan = table.getScanner(c, HConstants.EMPTY_START_ROW);

    VectorMapWritable<Integer, DoubleEntry> trunk = new VectorMapWritable<Integer, DoubleEntry>();

    for (RowResult row : scan) {
      trunk.put(BytesUtil.bytesToInt(row.getRow()), 
          new DoubleEntry(row.get(columnKey)));
    }

    return new DenseVector(trunk);
  }

  public Matrix mult(Matrix B) throws IOException {
    Matrix result = new DenseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    SIMDMultiplyMap.initJob(this.getPath(), B.getPath(), SIMDMultiplyMap.class,
        IntWritable.class, VectorWritable.class, jobConf);
    RowCyclicReduce.initJob(result.getPath(), SIMDMultiplyReduce.class, jobConf);
    JobManager.execute(jobConf, result);
    return result;
  }

  public Matrix multAdd(double alpha, Matrix B, Matrix C) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public double norm(Norm type) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  public Matrix set(double alpha, Matrix B) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public Matrix set(Matrix B) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public void setRow(int row, Vector vector) throws IOException {
    VectorUpdate update = new VectorUpdate(row);
    update.putAll(((DenseVector) vector).getEntries().entrySet());
    table.commit(update.getBatchUpdate());
  }

  public void setColumn(int column, Vector vector) throws IOException {
    // TODO Auto-generated method stub
  }

  public String getType() {
    return this.getClass().getSimpleName();
  }

  public SubMatrix subMatrix(int i0, int i1, int j0, int j1) throws IOException {
    int columnSize = (j1 - j0) + 1;
    SubMatrix result = new SubMatrix((i1-i0) + 1, columnSize);
    byte[][] c = new byte[columnSize][];
    for (int i = 0; i < columnSize; i++) {
      c[i] = BytesUtil.getColumnIndex(j0 + i);
    }

    Scanner scan = table.getScanner(c, BytesUtil.intToBytes(i0), BytesUtil.intToBytes(i1 + 1));

    int rKey = 0, cKey = 0;
    for (RowResult row : scan) {
      for (Map.Entry<byte[], Cell> e : row.entrySet()) {
        result.set(rKey, cKey, BytesUtil.bytesToDouble(e.getValue().getValue()));
        cKey++;
      }
      rKey++;
      cKey = 0;
    }

    return result;
  }
}
