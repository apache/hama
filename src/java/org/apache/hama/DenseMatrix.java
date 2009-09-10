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
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hama.algebra.BlockMultiplyMap;
import org.apache.hama.algebra.BlockMultiplyReduce;
import org.apache.hama.algebra.DenseMatrixVectorMultMap;
import org.apache.hama.algebra.DenseMatrixVectorMultReduce;
import org.apache.hama.algebra.JacobiEigenValue;
import org.apache.hama.algebra.RowCyclicAdditionMap;
import org.apache.hama.algebra.RowCyclicAdditionReduce;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.io.Pair;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.mapred.CollectBlocksMapper;
import org.apache.hama.mapred.DummyMapper;
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
   * Construct a raw matrix. Just create a table in HBase.
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @throws IOException throw the exception to let the user know what happend,
   *                 if we didn't create the matrix successfully.
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
    DenseMatrix rand = new DenseMatrix(conf, m, n);
    LOG.info("Create the " + m + " * " + n + " random matrix : "
        + rand.getPath());

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
    jobConf.setInt("matrix.column", n);
    jobConf.set("matrix.type", TABLE_PREFIX);
    jobConf.set("matrix.density", "100");

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
    return new DenseVector(table.getRow(BytesUtil.getRowIndex(i),
        new byte[][] { Bytes.toBytes(Constants.COLUMN) }));
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
    if (this.getRows() < i || this.getColumns() < j)
      throw new ArrayIndexOutOfBoundsException(i + ", " + j);
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
    if (this.getRows() < row || this.getColumns() < vector.size())
      throw new ArrayIndexOutOfBoundsException(row);

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
    if (this.getColumns() < column || this.getRows() < vector.size())
      throw new ArrayIndexOutOfBoundsException(column);

    for (Map.Entry<Writable, Writable> e : vector.getEntries().entrySet()) {
      int key = ((IntWritable) e.getKey()).get();
      double value = ((DoubleEntry) e.getValue()).getValue();
      VectorUpdate update = new VectorUpdate(key);
      update.put(column, value);
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

    DenseMatrix result = new DenseMatrix(config, this.getRows(), this
        .getColumns());
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job" + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    RowCyclicAdditionMap.initJob(this.getPath(), B.getPath(), Double
        .toString(alpha), RowCyclicAdditionMap.class, IntWritable.class,
        MapWritable.class, jobConf);
    RowCyclicAdditionReduce.initJob(result.getPath(),
        RowCyclicAdditionReduce.class, jobConf);

    JobManager.execute(jobConf);
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

    DenseMatrix result = new DenseMatrix(config, this.getRows(), this
        .getColumns());
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

    JobManager.execute(jobConf);
    return result;
  }

  private void ensureForAddition(Matrix m) throws IOException {
    if (getRows() != m.getRows() || getColumns() != m.getColumns()) {
      throw new IOException(
          "Matrices' rows and columns should be same while A+B.");
    }
  }

  /**
   * C = A*B using iterative method
   * 
   * @param B
   * @return C
   * @throws IOException
   */
  public DenseMatrix mult(Matrix B) throws IOException {
    ensureForMultiplication(B);
    int columns = 0;
    if(B.getColumns() == 1 || this.getColumns() == 1)
      columns = 1;
    else
      columns = this.getColumns();
    
    DenseMatrix result = new DenseMatrix(config, this.getRows(), columns);

    for (int i = 0; i < this.getRows(); i++) {
      JobConf jobConf = new JobConf(config);
      jobConf.setJobName("multiplication MR job : " + result.getPath() + " "
          + i);

      jobConf.setNumMapTasks(config.getNumMapTasks());
      jobConf.setNumReduceTasks(config.getNumReduceTasks());

      DenseMatrixVectorMultMap.initJob(i, this.getPath(), B.getPath(),
          DenseMatrixVectorMultMap.class, IntWritable.class, MapWritable.class,
          jobConf);
      DenseMatrixVectorMultReduce.initJob(result.getPath(),
          DenseMatrixVectorMultReduce.class, jobConf);
      JobManager.execute(jobConf);
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
  public DenseMatrix mult(Matrix B, int blocks) throws IOException {
    ensureForMultiplication(B);

    String collectionTable = "collect_" + RandomVariable.randMatrixPath();
    HTableDescriptor desc = new HTableDescriptor(collectionTable);
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes(Constants.BLOCK)));
    this.admin.createTable(desc);
    LOG.info("Collect Blocks");

    collectBlocksMapRed(this.getPath(), collectionTable, blocks, true);
    collectBlocksMapRed(B.getPath(), collectionTable, blocks, false);

    DenseMatrix result = new DenseMatrix(config, this.getRows(), this
        .getColumns());

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    BlockMultiplyMap.initJob(collectionTable, BlockMultiplyMap.class,
        BlockID.class, BlockWritable.class, jobConf);
    BlockMultiplyReduce.initJob(result.getPath(), BlockMultiplyReduce.class,
        jobConf);

    JobManager.execute(jobConf);
    hamaAdmin.delete(collectionTable);
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
    if (type == Norm.One)
      return getNorm1();
    else if (type == Norm.Frobenius)
      return getFrobenius();
    else if (type == Norm.Infinity)
      return getInfinity();
    else
      return getMaxvalue();
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
  public void collectBlocksMapRed(String path, String collectionTable,
      int blockNum, boolean bool) throws IOException {
    double blocks = Math.pow(blockNum, 0.5);
    if (!String.valueOf(blocks).endsWith(".0"))
      throw new IOException("can't divide.");

    int block_size = (int) blocks;
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("Blocking MR job" + getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());
    jobConf.setMapperClass(CollectBlocksMapper.class);
    jobConf.setInputFormat(VectorInputFormat.class);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);

    FileInputFormat.addInputPaths(jobConf, path);

    CollectBlocksMapper.initJob(collectionTable, bool, block_size, this
        .getRows(), this.getColumns(), jobConf);

    JobManager.execute(jobConf);
  }

  /**
   * Compute all the eigen values. Note: all the eigen values are collected in
   * the "eival:value" column, and the eigen vector of a specified eigen value
   * is collected in the "eivec:" column family in the same row.
   * 
   * TODO: we may need to expose the interface to access the eigen values and
   * vectors
   * 
   * @param loops limit the loops of the computation
   * @throws IOException
   */
  public void jacobiEigenValue(int loops) throws IOException {
    JobConf jobConf = new JobConf(config);

    /***************************************************************************
     * Initialization
     * 
     * A M/R job is used for initialization(such as, preparing a matrx copy of
     * the original in "eicol:" family.)
     **************************************************************************/
    // initialization
    jobConf.setJobName("JacobiEigen initialization MR job" + getPath());

    jobConf.setMapperClass(JacobiEigenValue.InitMapper.class);
    jobConf.setInputFormat(VectorInputFormat.class);
    jobConf.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);

    FileInputFormat.addInputPaths(jobConf, getPath());
    jobConf.set(JacobiEigenValue.MATRIX, getPath());
    jobConf.setOutputFormat(NullOutputFormat.class);
    jobConf.setMapOutputKeyClass(IntWritable.class);
    jobConf.setMapOutputValueClass(MapWritable.class);

    JobManager.execute(jobConf);

    final FileSystem fs = FileSystem.get(jobConf);
    Pair pivotPair = new Pair();
    DoubleWritable pivotWritable = new DoubleWritable();
    VectorUpdate vu;

    // loop
    int size = this.getRows();
    int state = size;
    int pivot_row, pivot_col;
    double pivot;
    double s, c, t, y;

    while (state != 0 && loops > 0) {
      /*************************************************************************
       * Find the pivot and its index(pivot_row, pivot_col)
       * 
       * A M/R job is used to scan all the "eival:ind" to get the max absolute
       * value of each row, and do a MAX aggregation of these max values to get
       * the max value in the matrix.
       ************************************************************************/
      jobConf = new JobConf(config);
      jobConf.setJobName("Find Pivot MR job" + getPath());

      jobConf.setNumReduceTasks(1);

      Path outDir = new Path(new Path(getType() + "_TMP_FindPivot_dir_"
          + System.currentTimeMillis()), "out");
      if (fs.exists(outDir))
        fs.delete(outDir, true);

      jobConf.setMapperClass(JacobiEigenValue.PivotMapper.class);
      jobConf.setInputFormat(JacobiEigenValue.PivotInputFormat.class);
      jobConf.set(JacobiEigenValue.PivotInputFormat.COLUMN_LIST,
          JacobiEigenValue.EIIND);
      FileInputFormat.addInputPaths(jobConf, getPath());
      jobConf.setMapOutputKeyClass(Pair.class);
      jobConf.setMapOutputValueClass(DoubleWritable.class);

      jobConf.setOutputKeyClass(Pair.class);
      jobConf.setOutputValueClass(DoubleWritable.class);
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);
      FileOutputFormat.setOutputPath(jobConf, outDir);

      // update the out put dir of the job
      outDir = FileOutputFormat.getOutputPath(jobConf);

      JobManager.execute(jobConf);

      // read outputs
      Path inFile = new Path(outDir, "part-00000");
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, jobConf);
      try {
        reader.next(pivotPair, pivotWritable);
        pivot_row = pivotPair.getRow();
        pivot_col = pivotPair.getColumn();
        pivot = pivotWritable.get();
      } finally {
        reader.close();
      }
      fs.delete(outDir.getParent(), true);

      /*************************************************************************
       * Calculation
       * 
       * Compute the rotation parameters of next rotation.
       ************************************************************************/
      double e1 = BytesUtil.bytesToDouble(table.get(
          BytesUtil.getRowIndex(pivot_row),
          Bytes.toBytes(JacobiEigenValue.EIVAL)).getValue());
      double e2 = BytesUtil.bytesToDouble(table.get(
          BytesUtil.getRowIndex(pivot_col),
          Bytes.toBytes(JacobiEigenValue.EIVAL)).getValue());

      y = (e2 - e1) / 2;
      t = Math.abs(y) + Math.sqrt(pivot * pivot + y * y);
      s = Math.sqrt(pivot * pivot + t * t);
      c = t / s;
      s = pivot / s;
      t = (pivot * pivot) / t;
      if (y < 0) {
        s = -s;
        t = -t;
      }

      /*************************************************************************
       * Upate the pivot and the eigen values indexed by the pivot
       ************************************************************************/
      vu = new VectorUpdate(pivot_row);
      vu.put(JacobiEigenValue.EICOL, pivot_col, 0);
      table.commit(vu.getBatchUpdate());

      state = update(pivot_row, -t, state);
      state = update(pivot_col, t, state);

      /*************************************************************************
       * Rotation the matrix
       ************************************************************************/
      // rotation
      jobConf = new JobConf(config);
      jobConf.setJobName("Rotation Matrix MR job" + getPath());

      jobConf.setInt(JacobiEigenValue.PIVOTROW, pivot_row);
      jobConf.setInt(JacobiEigenValue.PIVOTCOL, pivot_col);
      jobConf.set(JacobiEigenValue.PIVOTSIN, String.valueOf(s));
      jobConf.set(JacobiEigenValue.PIVOTCOS, String.valueOf(c));

      jobConf.setMapperClass(DummyMapper.class);
      jobConf.setInputFormat(JacobiEigenValue.RotationInputFormat.class);
      jobConf.set(JacobiEigenValue.RotationInputFormat.COLUMN_LIST,
          JacobiEigenValue.EIIND);
      FileInputFormat.addInputPaths(jobConf, getPath());
      jobConf.setMapOutputKeyClass(NullWritable.class);
      jobConf.setMapOutputValueClass(NullWritable.class);
      FileInputFormat.addInputPaths(jobConf, getPath());
      jobConf.setOutputFormat(NullOutputFormat.class);

      JobManager.execute(jobConf);

      // rotate eigenvectors
      LOG.info("rotating eigenvector");
      for (int i = 0; i < size; i++) {
        e1 = BytesUtil.bytesToDouble(table.get(
            BytesUtil.getRowIndex(pivot_row),
            Bytes.toBytes(JacobiEigenValue.EIVEC + i)).getValue());
        e2 = BytesUtil.bytesToDouble(table.get(
            BytesUtil.getRowIndex(pivot_col),
            Bytes.toBytes(JacobiEigenValue.EIVEC + i)).getValue());

        vu = new VectorUpdate(pivot_row);
        vu.put(JacobiEigenValue.EIVEC, i, c * e1 - s * e2);
        table.commit(vu.getBatchUpdate());

        vu = new VectorUpdate(pivot_col);
        vu.put(JacobiEigenValue.EIVEC, i, s * e1 + c * e2);
        table.commit(vu.getBatchUpdate());
      }

      LOG.info("update index...");
      // update index array
      maxind(pivot_row, size);
      maxind(pivot_col, size);

      loops--;
    }
  }

  void maxind(int row, int size) throws IOException {
    int m = row + 1;
    if (row + 2 < size) {
      double max = BytesUtil.bytesToDouble(table
          .get(BytesUtil.getRowIndex(row),
              Bytes.toBytes(JacobiEigenValue.EICOL + m)).getValue());
      double val;
      for (int i = row + 2; i < size; i++) {
        val = BytesUtil.bytesToDouble(table.get(BytesUtil.getRowIndex(row),
            Bytes.toBytes(JacobiEigenValue.EICOL + i)).getValue());
        if (Math.abs(val) > Math.abs(max)) {
          m = i;
          max = val;
        }
      }
    }

    VectorUpdate vu = new VectorUpdate(row);
    vu.put(JacobiEigenValue.EIIND, m);
    table.commit(vu.getBatchUpdate());
  }

  int update(int row, double value, int state) throws IOException {
    double e = BytesUtil.bytesToDouble(table.get(BytesUtil.getRowIndex(row),
        Bytes.toBytes(JacobiEigenValue.EIVAL)).getValue());
    int changed = BytesUtil.bytesToInt(table.get(BytesUtil.getRowIndex(row),
        Bytes.toBytes(JacobiEigenValue.EICHANGED)).getValue());
    double y = e;
    e += value;

    VectorUpdate vu = new VectorUpdate(row);
    vu.put(JacobiEigenValue.EIVAL, e);
    if (changed == 1 && (Math.abs(y - e) < .0000001)) { // y == e) {
      changed = 0;
      vu.put(JacobiEigenValue.EICHANGED, changed);
      state--;
    } else if (changed == 0 && (Math.abs(y - e) > .0000001)) {
      changed = 1;
      vu.put(JacobiEigenValue.EICHANGED, changed);
      state++;
    }
    table.commit(vu.getBatchUpdate());
    return state;
  }

  // for test
  boolean verifyEigenValue(double[] e, double[][] E) throws IOException {
    boolean success = true;
    double e1, ev;
    for (int i = 0; i < e.length; i++) {
      e1 = BytesUtil.bytesToDouble(table.get(BytesUtil.getRowIndex(i),
          Bytes.toBytes(JacobiEigenValue.EIVAL)).getValue());
      success &= ((Math.abs(e1 - e[i]) < .0000001));
      if (!success)
        return success;

      for (int j = 0; j < E[i].length; j++) {
        ev = BytesUtil.bytesToDouble(table.get(BytesUtil.getRowIndex(i),
            Bytes.toBytes(JacobiEigenValue.EIVEC + j)).getValue());
        success &= ((Math.abs(ev - E[i][j]) < .0000001));
        if (!success)
          return success;
      }
    }
    return success;
  }
}
