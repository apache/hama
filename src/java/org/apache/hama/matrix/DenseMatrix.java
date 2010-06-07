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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.RandomVariable;

/**
 * This class represents a dense matrix.
 */
public class DenseMatrix extends AbstractMatrix implements Matrix {
  static private final String TABLE_PREFIX = DenseMatrix.class.getSimpleName();

  /**
   * Construct a raw matrix. Just create a table in HBase.
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @throws IOException throw the exception to let the user know what happend,
   *           if we didn't create the matrix successfully.
   */
  public DenseMatrix(HamaConfiguration conf, int m, int n) throws IOException {
    setConfiguration(conf);

    tryToCreateTable(TABLE_PREFIX);
    closed = false;
    this.setDimension(m, n);
  }

  /**
   * Create/load a matrix aliased as 'matrixName'.
   * 
   * @param conf configuration object
   * @param matrixName the name of the matrix
   * @param force if force is true, a new matrix will be created no matter
   *          'matrixName' has aliased to an existed matrix; otherwise, just try
   *          to load an existed matrix alised 'matrixName'.
   * @throws IOException
   */
  public DenseMatrix(HamaConfiguration conf, String matrixName, boolean force)
      throws IOException {
    setConfiguration(conf);
    // if force is set to true:
    // 1) if this matrixName has aliase to other matrix, we will remove
    // the old aliase, create a new matrix table, and aliase to it.

    // 2) if this matrixName has no aliase to other matrix, we will create
    // a new matrix table, and alise to it.
    //
    // if force is set to false, we just try to load an existed matrix alised
    // as 'matrixname'.

    boolean existed = hamaAdmin.matrixExists(matrixName);

    if (force) {
      if (existed) {
        // remove the old aliase
        hamaAdmin.delete(matrixName);
      }
      // create a new matrix table.
      tryToCreateTable(TABLE_PREFIX);
      // save the new aliase relationship
      save(matrixName);
    } else {
      if (existed) {
        // try to get the actual path of the table
        matrixPath = hamaAdmin.getPath(matrixName);
        // load the matrix
        table = new HTable(conf, matrixPath);
        // increment the reference
        incrementAndGetRef();
      } else {
        throw new IOException("Try to load non-existed matrix alised as "
            + matrixName);
      }
    }

    closed = false;
  }

  /**
   * Load a matrix from an existed matrix table whose tablename is 'matrixpath'
   * !! It is an internal used for map/reduce.
   * 
   * @param conf configuration object
   * @param matrixpath
   * @throws IOException
   * @throws IOException
   */
  public DenseMatrix(HamaConfiguration conf, String matrixpath)
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
   * Create an m-by-n constant matrix.
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @param s fill the matrix with this scalar value.
   * @throws IOException throw the exception to let the user know what happend,
   *           if we didn't create the matrix successfully.
   */
  public DenseMatrix(HamaConfiguration conf, int m, int n, double s)
      throws IOException {
    setConfiguration(conf);

    tryToCreateTable(TABLE_PREFIX);

    closed = false;

    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        set(i, j, s);
      }
    }

    setDimension(m, n);
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
  public static DenseMatrix random(HamaConfiguration conf, int m, int n)
      throws IOException {
    DenseMatrix rand = new DenseMatrix(conf, m, n);
    DenseVector vector = new DenseVector();
    LOG.info("Create the " + m + " * " + n + " random matrix : "
        + rand.getPath());

    for (int i = 0; i < m; i++) {
      vector.clear();
      for (int j = 0; j < n; j++) {
        vector.set(j, RandomVariable.rand());
      }
      rand.setRow(i, vector);
    }

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
  public static DenseMatrix identity(HamaConfiguration conf, int m, int n)
      throws IOException {
    DenseMatrix identity = new DenseMatrix(conf, m, n);
    LOG.info("Create the " + m + " * " + n + " identity matrix : "
        + identity.getPath());

    for (int i = 0; i < m; i++) {
      DenseVector vector = new DenseVector();
      for (int j = 0; j < n; j++) {
        vector.set(j, (i == j ? 1.0 : 0.0));
      }
      identity.setRow(i, vector);
    }

    return identity;
  }

  /**
   * Gets the double value of (i, j)
   * 
   * @param i ith row of the matrix
   * @param j jth column of the matrix
   * @return the value of entry, or zero If entry is null
   * @throws IOException
   */
  public double get(int i, int j) throws IOException {
    if (this.getRows() < i || this.getColumns() < j)
      throw new ArrayIndexOutOfBoundsException(i + ", " + j);

    Get get = new Get(BytesUtil.getRowIndex(i));
    get.addColumn(Constants.COLUMNFAMILY);
    byte[] result = table.get(get).getValue(Constants.COLUMNFAMILY,
        Bytes.toBytes(String.valueOf(j)));

    if (result == null)
      throw new NullPointerException("Unexpected null");

    return Bytes.toDouble(result);
  }

  /**
   * Gets the vector of row
   * 
   * @param i the row index of the matrix
   * @return the vector of row
   * @throws IOException
   */
  public DenseVector getRow(int i) throws IOException {
    Get get = new Get(BytesUtil.getRowIndex(i));
    get.addFamily(Constants.COLUMNFAMILY);
    Result r = table.get(get);
    return new DenseVector(r);
  }

  /**
   * Gets the vector of column
   * 
   * @param j the column index of the matrix
   * @return the vector of column
   * @throws IOException
   */
  public DenseVector getColumn(int j) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j)));
    ResultScanner s = table.getScanner(scan);
    Result r = null;

    MapWritable trunk = new MapWritable();
    while ((r = s.next()) != null) {
      byte[] value = r.getValue(Constants.COLUMNFAMILY, Bytes.toBytes(String
          .valueOf(j)));
      LOG.info(Bytes.toString(r.getRow()));
      trunk.put(new IntWritable(BytesUtil.getRowIndex(r.getRow())),
          new DoubleWritable(Bytes.toDouble(value)));
    }

    return new DenseVector(trunk);
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double value) throws IOException {
    if (this.getRows() < i || this.getColumns() < j)
      throw new ArrayIndexOutOfBoundsException(this.getRows() + ", "
          + this.getColumns() + ": " + i + ", " + j);
    Put put = new Put(BytesUtil.getRowIndex(i));
    put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j)), Bytes
        .toBytes(value));
    table.put(put);
  }

  /**
   * Set the row of a matrix to a given vector
   * 
   * @param row
   * @param vector
   * @throws IOException
   */
  public void setRow(int row, Vector vector) throws IOException {
    if (this.getRows() < row || this.getColumns() < vector.size())
      throw new ArrayIndexOutOfBoundsException(row);
    Put put = new Put(BytesUtil.getRowIndex(row));
    for (Map.Entry<Writable, Writable> e : vector.getEntries().entrySet()) {
      put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String
          .valueOf(((IntWritable) e.getKey()).get())), Bytes
          .toBytes(((DoubleWritable) e.getValue()).get()));
    }
    table.put(put);
  }

  /**
   * Set the column of a matrix to a given vector
   * 
   * @param column
   * @param vector
   * @throws IOException
   */
  public void setColumn(int column, Vector vector) throws IOException {
    if (this.getColumns() < column || this.getRows() < vector.size())
      throw new ArrayIndexOutOfBoundsException(column);

    for (Map.Entry<Writable, Writable> e : vector.getEntries().entrySet()) {
      int key = ((IntWritable) e.getKey()).get();
      double value = ((DoubleWritable) e.getValue()).get();
      Put put = new Put(BytesUtil.getRowIndex(key));
      put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(column)),
          Bytes.toBytes(value));
      table.put(put);
    }
  }

  /**
   * C = alpha*A*B + C
   * 
   * @param alpha
   * @param B
   * @param C
   * @return C
   * @throws IOException
   */
  public Matrix multAdd(double alpha, Matrix B, Matrix C) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Returns type of matrix
   */
  public String getType() {
    return this.getClass().getSimpleName();
  }

  /**
   * Returns the sub matrix formed by selecting certain rows and columns from a
   * bigger matrix. The sub matrix is a in-memory operation only.
   * 
   * @param i0 the start index of row
   * @param i1 the end index of row
   * @param j0 the start index of column
   * @param j1 the end index of column
   * @return the sub matrix of matrix
   * @throws IOException
   */
  public SubMatrix subMatrix(int i0, int i1, int j0, int j1) throws IOException {
    int columnSize = (j1 - j0) + 1;
    SubMatrix result = new SubMatrix((i1 - i0) + 1, columnSize);

    Scan scan = new Scan();
    for (int j = j0, jj = 0; j <= j1; j++, jj++) {
      scan.addColumn(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j)));
    }
    scan.setStartRow(BytesUtil.getRowIndex(i0));
    scan.setStopRow(BytesUtil.getRowIndex(i1 + 1));
    ResultScanner s = table.getScanner(scan);
    Iterator<Result> it = s.iterator();

    int i = 0;
    Result rs = null;
    while (it.hasNext()) {
      rs = it.next();
      for (int j = j0, jj = 0; j <= j1; j++, jj++) {
        byte[] vv = rs.getValue(Constants.COLUMNFAMILY, Bytes.toBytes(String
            .valueOf(j)));
        result.set(i, jj, vv);
      }
      i++;
    }

    return result;
  }
}
