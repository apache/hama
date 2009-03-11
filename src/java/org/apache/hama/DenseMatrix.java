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
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hama.algebra.BlockMultiplyMap;
import org.apache.hama.algebra.BlockMultiplyReduce;
import org.apache.hama.algebra.RowCyclicAdditionMap;
import org.apache.hama.algebra.RowCyclicAdditionReduce;
import org.apache.hama.algebra.SIMDMultiplyMap;
import org.apache.hama.algebra.SIMDMultiplyReduce;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.CollectBlocksMapper;
import org.apache.hama.mapred.RandomMatrixMap;
import org.apache.hama.mapred.RandomMatrixReduce;
import org.apache.hama.mapred.VectorInputFormat;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.JobManager;
import org.apache.hama.util.RandomVariable;

/**
 * This class represents a dense matrix.
 */
public class DenseMatrix extends AbstractMatrix implements Matrix {
  static private final String TABLE_PREFIX = DenseMatrix.class.getSimpleName();
  static private final Path TMP_DIR = new Path(DenseMatrix.class
      .getSimpleName()
      + "_TMP_dir");

  /**
   * Construct a raw matrix. Just create a table in HBase, but didn't lay any
   * schema ( such as dimensions: i, j ) on it.
   * 
   * @param conf configuration object
   * @throws IOException throw the exception to let the user know what happend,
   *                 if we didn't create the matrix successfully.
   */
  public DenseMatrix(HamaConfiguration conf) throws IOException {
    setConfiguration(conf);

    tryToCreateTable(TABLE_PREFIX);

    closed = false;
  }

  /**
   * Create/load a matrix aliased as 'matrixName'.
   * 
   * @param conf configuration object
   * @param matrixName the name of the matrix
   * @param force if force is true, a new matrix will be created no matter
   *                'matrixName' has aliased to an existed matrix; otherwise,
   *                just try to load an existed matrix alised 'matrixName'.
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
   * Load a matrix from an existed matrix table whose tablename is 'matrixpath' !!
   * It is an internal used for map/reduce.
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
   *                 if we didn't create the matrix successfully.
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
    DenseMatrix rand = new DenseMatrix(conf);
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

    rand.setDimension(m, n);
    return rand;
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
    DenseMatrix rand = new DenseMatrix(conf);
    LOG.info("Create the " + m + " * " + n + " random matrix : "
        + rand.getPath());
    rand.setDimension(m, n);

    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("random matrix MR job : " + rand.getPath());

    jobConf.setNumMapTasks(conf.getNumMapTasks());
    jobConf.setNumReduceTasks(conf.getNumReduceTasks());

    final Path inDir = new Path(TMP_DIR, "in");
    FileInputFormat.setInputPaths(jobConf, inDir);
    jobConf.setMapperClass(RandomMatrixMap.class);
    jobConf.setMapOutputKeyClass(IntWritable.class);
    jobConf.setMapOutputValueClass(MapWritable.class);

    RandomMatrixReduce.initJob(rand.getPath(), RandomMatrixReduce.class,
        jobConf);
    jobConf.setSpeculativeExecution(false);
    jobConf.set("matrix.column", String.valueOf(n));

    jobConf.setInputFormat(SequenceFileInputFormat.class);
    final FileSystem fs = FileSystem.get(jobConf);
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
      final SequenceFile.Writer writer = SequenceFile.createWriter(fs, jobConf,
          file, IntWritable.class, IntWritable.class, CompressionType.NONE);
      try {
        writer.append(start, end);
      } finally {
        writer.close();
      }
      System.out.println("Wrote input for Map #" + i);
    }

    JobClient.runJob(jobConf);
    fs.delete(TMP_DIR, true);
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
    DenseMatrix identity = new DenseMatrix(conf);
    LOG.info("Create the " + m + " * " + n + " identity matrix : "
        + identity.getPath());

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

    Cell c = table.get(BytesUtil.getRowIndex(i), BytesUtil.getColumnIndex(j));
    if (c == null)
      throw new NullPointerException("Unexpected null");

    return BytesUtil.bytesToDouble(c.getValue());
  }

  /**
   * Gets the vector of row
   * 
   * @param i the row index of the matrix
   * @return the vector of row
   * @throws IOException
   */
  public DenseVector getRow(int i) throws IOException {
    return new DenseVector(table.getRow(BytesUtil.getRowIndex(i)));
  }

  /**
   * Gets the vector of column
   * 
   * @param j the column index of the matrix
   * @return the vector of column
   * @throws IOException
   */
  public DenseVector getColumn(int j) throws IOException {
    byte[] columnKey = BytesUtil.getColumnIndex(j);
    byte[][] c = { columnKey };
    Scanner scan = table.getScanner(c, HConstants.EMPTY_START_ROW);

    MapWritable trunk = new MapWritable();

    for (RowResult row : scan) {
      trunk.put(new IntWritable(BytesUtil.bytesToInt(row.getRow())),
          new DoubleEntry(row.get(columnKey)));
    }

    return new DenseVector(trunk);
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double value) throws IOException {
    VectorUpdate update = new VectorUpdate(i);
    update.put(j, value);
    table.commit(update.getBatchUpdate());
  }

  /**
   * Set the row of a matrix to a given vector
   * 
   * @param row
   * @param vector
   * @throws IOException
   */
  public void setRow(int row, Vector vector) throws IOException {
    if (this.getRows() < row)
      increaseRows();

    VectorUpdate update = new VectorUpdate(row);
    update.putAll(vector.getEntries());
    table.commit(update.getBatchUpdate());
  }

  /**
   * Set the column of a matrix to a given vector
   * 
   * @param column
   * @param vector
   * @throws IOException
   */
  public void setColumn(int column, Vector vector) throws IOException {
    if (this.getColumns() < column)
      increaseColumns();

    for (int i = 0; i < vector.size(); i++) {
      VectorUpdate update = new VectorUpdate(i);
      update.put(column, vector.get(i));
      table.commit(update.getBatchUpdate());
    }
  }

  /**
   * C = alpha*B + A
   * 
   * @param alpha
   * @param B
   * @return C
   * @throws IOException
   */
  public DenseMatrix add(double alpha, Matrix B) throws IOException {
    ensureForAddition(B);

    DenseMatrix result = new DenseMatrix(config);
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job" + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    RowCyclicAdditionMap.initJob(this.getPath(), B.getPath(), Double
        .toString(alpha), RowCyclicAdditionMap.class, IntWritable.class,
        MapWritable.class, jobConf);
    RowCyclicAdditionReduce.initJob(result.getPath(),
        RowCyclicAdditionReduce.class, jobConf);

    JobManager.execute(jobConf, result);
    return result;
  }

  /**
   * C = B + A
   * 
   * @param B
   * @return C
   * @throws IOException
   */
  public DenseMatrix add(Matrix B) throws IOException {
    return add(1.0, B);
  }

  public DenseMatrix add(Matrix... matrices) throws IOException {
    // ensure all the matrices are suitable for addition.
    for (Matrix m : matrices) {
      ensureForAddition(m);
    }

    DenseMatrix result = new DenseMatrix(config);
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job" + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    StringBuilder summandList = new StringBuilder();
    StringBuilder alphaList = new StringBuilder();
    for (Matrix m : matrices) {
      summandList.append(m.getPath());
      summandList.append(",");
      alphaList.append("1");
      alphaList.append(",");
    }
    summandList.deleteCharAt(summandList.length() - 1);
    alphaList.deleteCharAt(alphaList.length() - 1);

    RowCyclicAdditionMap.initJob(this.getPath(), summandList.toString(),
        alphaList.toString(), RowCyclicAdditionMap.class, IntWritable.class,
        MapWritable.class, jobConf);
    RowCyclicAdditionReduce.initJob(result.getPath(),
        RowCyclicAdditionReduce.class, jobConf);

    JobManager.execute(jobConf, result);
    return result;
  }

  private void ensureForAddition(Matrix m) throws IOException {
    if (getRows() != m.getRows() || getColumns() != m.getColumns()) {
      throw new IOException(
          "Matrices' rows and columns should be same while A+B.");
    }
  }

  /**
   * C = A*B using SIMD algorithm
   * 
   * @param B
   * @return C
   * @throws IOException
   */
  public DenseMatrix mult(Matrix B) throws IOException {
    ensureForMultiplication(B);

    DenseMatrix result = new DenseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    SIMDMultiplyMap.initJob(this.getPath(), B.getPath(), this.getType(),
        SIMDMultiplyMap.class, IntWritable.class, MapWritable.class, jobConf);
    SIMDMultiplyReduce.initJob(result.getPath(), SIMDMultiplyReduce.class,
        jobConf);
    JobManager.execute(jobConf, result);
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
  public DenseMatrix mult(Matrix B, int blocks) throws IOException {
    ensureForMultiplication(B);

    DenseMatrix collectionTable = new DenseMatrix(config);
    LOG.info("Collect Blocks");

    collectBlocksMapRed(this.getPath(), collectionTable, blocks, true);
    collectBlocksMapRed(B.getPath(), collectionTable, blocks, false);

    DenseMatrix result = new DenseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    BlockMultiplyMap.initJob(collectionTable.getPath(), BlockMultiplyMap.class,
        BlockID.class, BlockWritable.class, jobConf);
    BlockMultiplyReduce.initJob(result.getPath(), BlockMultiplyReduce.class,
        jobConf);

    JobManager.execute(jobConf, result);
    // Should be collectionTable removed?
    return result;
  }

  private void ensureForMultiplication(Matrix m) throws IOException {
    if (getColumns() != m.getRows()) {
      throw new IOException("A's columns should equal with B's rows while A*B.");
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
   * Computes the given norm of the matrix
   * 
   * @param type
   * @return norm of the matrix
   * @throws IOException
   */
  public double norm(Norm type) throws IOException {
    // TODO Auto-generated method stub
    return 0;
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

    byte[][] cols = new byte[columnSize][];
    for (int j = j0, jj = 0; j <= j1; j++, jj++) {
      cols[jj] = BytesUtil.getColumnIndex(j);
    }

    Scanner scan = table.getScanner(cols, BytesUtil.getRowIndex(i0), BytesUtil
        .getRowIndex(i1 + 1));
    Iterator<RowResult> it = scan.iterator();
    int i = 0;
    RowResult rs = null;
    while (it.hasNext()) {
      rs = it.next();
      for (int j = j0, jj = 0; j <= j1; j++, jj++) {
        result.set(i, jj, rs.get(BytesUtil.getColumnIndex(j)).getValue());
      }
      i++;
    }

    scan.close();
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
  public void collectBlocksMapRed(String path, Matrix collectionTable,
      int blockNum, boolean bool) throws IOException {
    double blocks = Math.pow(blockNum, 0.5);
    if (!String.valueOf(blocks).endsWith(".0"))
      throw new IOException("can't divide.");

    int block_size = (int) blocks;
    collectionTable.setDimension(block_size, block_size);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("Blocking MR job" + getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());
    jobConf.setMapperClass(CollectBlocksMapper.class);
    jobConf.setInputFormat(VectorInputFormat.class);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);

    FileInputFormat.addInputPaths(jobConf, path);

    CollectBlocksMapper.initJob(collectionTable.getPath(), bool, block_size,
        this.getRows(), this.getColumns(), jobConf);

    JobManager.execute(jobConf);
  }
}
