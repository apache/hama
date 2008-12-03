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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hama.algebra.BlockCyclicMultiplyMap;
import org.apache.hama.algebra.BlockCyclicMultiplyReduce;
import org.apache.hama.algebra.RowCyclicAdditionMap;
import org.apache.hama.algebra.RowCyclicAdditionReduce;
import org.apache.hama.algebra.SIMDMultiplyMap;
import org.apache.hama.algebra.SIMDMultiplyReduce;
import org.apache.hama.io.BlockWritable;
import org.apache.hama.io.DoubleEntry;
import org.apache.hama.io.MapWritable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.mapred.BlockCyclicReduce;
import org.apache.hama.mapred.BlockingMapRed;
import org.apache.hama.mapred.RandomMatrixMap;
import org.apache.hama.mapred.RowCyclicReduce;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.JobManager;
import org.apache.hama.util.RandomVariable;

public class DenseMatrix extends AbstractMatrix implements Matrix {

  static int tryPathLength = Constants.DEFAULT_PATH_LENGTH;
  static final String TABLE_PREFIX = DenseMatrix.class.getSimpleName() + "_";
  static private final Path TMP_DIR = new Path(
      DenseMatrix.class.getSimpleName() + "_TMP_dir");
  
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

    tryToCreateTable();

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
      tryToCreateTable();
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

    tryToCreateTable();

    closed = false;

    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        set(i, j, s);
      }
    }

    setDimension(m, n);
  }

  /**
   * try to create a new matrix with a new random name. try times will be
   * (Integer.MAX_VALUE - 4) * DEFAULT_TRY_TIMES;
   * 
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
      if (tryTimes <= 0) { // this loop has exhausted DEFAULT_TRY_TIMES.
        tryPathLength++;
        tryTimes = Constants.DEFAULT_TRY_TIMES;
      }

    } while (tryPathLength <= Constants.DEFAULT_MAXPATHLEN);
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
  public static DenseMatrix random_mapred(HamaConfiguration conf, int m,
      int n) throws IOException {
    DenseMatrix rand = new DenseMatrix(conf);
    LOG.info("Create the " + m + " * " + n + " random matrix : "
        + rand.getPath());
    
    JobConf jobConf = new JobConf(conf);
    jobConf.setJobName("random matrix MR job : " + rand.getPath());

    jobConf.setNumMapTasks(conf.getNumMapTasks());
    jobConf.setNumReduceTasks(conf.getNumReduceTasks());

    final Path inDir = new Path(TMP_DIR, "in");
    FileInputFormat.setInputPaths(jobConf, inDir);
    jobConf.setMapperClass(RandomMatrixMap.class);

    jobConf.setOutputKeyClass(BooleanWritable.class);
    jobConf.setOutputValueClass(LongWritable.class);
    jobConf.setOutputFormat(NullOutputFormat.class);
    jobConf.setSpeculativeExecution(false);
    jobConf.set("matrix.column", String.valueOf(n));
    jobConf.set("matrix.path", rand.getPath());

    jobConf.setInputFormat(SequenceFileInputFormat.class);
    final FileSystem fs = FileSystem.get(jobConf);
    int interval = m/conf.getNumMapTasks();
    
    // generate an input file for each map task
    for (int i = 0; i < conf.getNumMapTasks(); ++i) {
      final Path file = new Path(inDir, "part" + i);
      final IntWritable start = new IntWritable(i * interval);
      IntWritable end = null;
      if((i + 1) != conf.getNumMapTasks()) {
        end = new IntWritable(((i * interval) + interval) - 1);        
      } else {
        end = new IntWritable(m);       
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
    Cell c = table.get(BytesUtil.intToBytes(i), BytesUtil.getColumnIndex(j));
    return (c != null) ? BytesUtil.bytesToDouble(c.getValue()) : 0;
  }

  public Matrix add(Matrix B) throws IOException {
    Matrix result = new DenseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job" + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    RowCyclicAdditionMap.initJob(this.getPath(), B.getPath(),
        RowCyclicAdditionMap.class, IntWritable.class, VectorWritable.class,
        jobConf);
    RowCyclicReduce.initJob(result.getPath(), RowCyclicAdditionReduce.class,
        jobConf);

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

    MapWritable<Integer, DoubleEntry> trunk = new MapWritable<Integer, DoubleEntry>();

    for (RowResult row : scan) {
      trunk.put(BytesUtil.bytesToInt(row.getRow()), new DoubleEntry(row
          .get(columnKey)));
    }

    return new DenseVector(trunk);
  }

  public Matrix mult(Matrix B) throws IOException {
    Matrix result = new DenseMatrix(config);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    if (this.isBlocked() && ((DenseMatrix) B).isBlocked()) {
      BlockCyclicMultiplyMap.initJob(this.getPath(), B.getPath(),
          BlockCyclicMultiplyMap.class, IntWritable.class, BlockWritable.class,
          jobConf);
      BlockCyclicReduce.initJob(result.getPath(),
          BlockCyclicMultiplyReduce.class, jobConf);
    } else {
      SIMDMultiplyMap.initJob(this.getPath(), B.getPath(),
          SIMDMultiplyMap.class, IntWritable.class, VectorWritable.class,
          jobConf);
      RowCyclicReduce.initJob(result.getPath(), SIMDMultiplyReduce.class,
          jobConf);
    }

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
    for (int i = 0; i < vector.size(); i++) {
      VectorUpdate update = new VectorUpdate(i);
      update.put(column, vector.get(i));
      table.commit(update.getBatchUpdate());
    }
  }

  public String getType() {
    return this.getClass().getSimpleName();
  }

  public SubMatrix subMatrix(int i0, int i1, int j0, int j1) throws IOException {
    int columnSize = (j1 - j0) + 1;
    SubMatrix result = new SubMatrix((i1 - i0) + 1, columnSize);

    for (int i = i0, ii = 0; i <= i1; i++, ii++) {
      for (int j = j0, jj = 0; j <= j1; j++, jj++) {
        Cell c = table
            .get(BytesUtil.intToBytes(i), BytesUtil.getColumnIndex(j));
        result.set(ii, jj, BytesUtil.bytesToDouble(c.getValue()));
      }
    }

    return result;
  }

  /**
   * Using a map/reduce job to block a dense matrix.
   * 
   * @param blockNum
   * @throws IOException
   */
  public void blocking_mapred(int blockNum) throws IOException {
    setBlockPosition(blockNum);
    setBlockSize(blockNum);
    LOG.info("Convert to " + blockNum + " * " + blockNum + " blocked matrix");
    
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("Blocking MR job" + getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    BlockingMapRed.initJob(getPath(), jobConf);

    JobManager.execute(jobConf);
  }

  public boolean isBlocked() throws IOException {
    return (table.get(Constants.METADATA, Constants.BLOCK_SIZE) == null) ? false
        : true;
  }

  public SubMatrix getBlock(int i, int j) throws IOException {
    return new SubMatrix(table.get(String.valueOf(i),
        Constants.BLOCK + j).getValue());
  }

  /**
   * @return the size of block
   * @throws IOException
   */
  public int getBlockSize() throws IOException {
    return (isBlocked()) ? BytesUtil.bytesToInt(table.get(Constants.METADATA,
        Constants.BLOCK_SIZE).getValue()) : -1;
  }

  protected void setBlockSize(int blockNum) throws IOException {
    BatchUpdate update = new BatchUpdate(Constants.METADATA);
    update.put(Constants.BLOCK_SIZE, BytesUtil.intToBytes(blockNum));
    table.commit(update);
  }

  public void setBlock(int i, int j, SubMatrix matrix) throws IOException {
    BatchUpdate update = new BatchUpdate(String.valueOf(i));
    update.put(Constants.BLOCK + j, matrix.getBytes());
    table.commit(update);
  }

  protected void setBlockPosition(int blockNum) throws IOException {
    int block_row_size = this.getRows() / blockNum;
    int block_column_size = this.getColumns() / blockNum;

    for (int i = 0; i < blockNum; i++) {
      for (int j = 0; j < blockNum; j++) {
        int startRow = i * block_row_size;
        int endRow = (startRow + block_row_size) - 1;
        int startColumn = j * block_column_size;
        int endColumn = (startColumn + block_column_size) - 1;

        BatchUpdate update = new BatchUpdate(getBlockKey(i, j));
        update.put(Constants.BLOCK_STARTROW, BytesUtil.intToBytes(startRow));
        update.put(Constants.BLOCK_ENDROW, BytesUtil.intToBytes(endRow));
        update.put(Constants.BLOCK_STARTCOLUMN, BytesUtil
            .intToBytes(startColumn));
        update.put(Constants.BLOCK_ENDCOLUMN, BytesUtil.intToBytes(endColumn));
        table.commit(update);
      }
    }
  }

  protected int[] getBlockPosition(int i, int j) throws IOException {
    int[] result = new int[4];
    result[0] = BytesUtil.bytesToInt(table.get(getBlockKey(i, j),
        Constants.BLOCK_STARTROW).getValue());
    result[1] = BytesUtil.bytesToInt(table.get(getBlockKey(i, j),
        Constants.BLOCK_ENDROW).getValue());
    result[2] = BytesUtil.bytesToInt(table.get(getBlockKey(i, j),
        Constants.BLOCK_STARTCOLUMN).getValue());
    result[3] = BytesUtil.bytesToInt(table.get(getBlockKey(i, j),
        Constants.BLOCK_ENDCOLUMN).getValue());
    return result;
  }

  protected String getBlockKey(int i, int j) {
    return i + "," + j;
  }

  /**
   * Using a scanner to block a dense matrix. If the matrix is large, use the
   * blocking_mapred()
   * 
   * @param blockNum
   * @throws IOException
   */
  public void blocking(int blockNum) throws IOException {
    setBlockPosition(blockNum);
    setBlockSize(blockNum);

    String[] columns = new String[] { Constants.BLOCK_STARTROW,
        Constants.BLOCK_ENDROW, Constants.BLOCK_STARTCOLUMN,
        Constants.BLOCK_ENDCOLUMN };
    Scanner scan = table.getScanner(columns);

    for (RowResult row : scan) {
      String[] key = Bytes.toString(row.getRow()).split("[,]");
      int blockR = Integer.parseInt(key[0]);
      int blockC = Integer.parseInt(key[1]);
      int[] pos = getBlockPosition(blockR, blockC);
      setBlock(blockR, blockC, subMatrix(pos[0], pos[1], pos[2], pos[3]));
    }
  }
}
