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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hama.Constants;
import org.apache.hama.HamaAdmin;
import org.apache.hama.HamaAdminImpl;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.algebra.MatrixNormMapReduce;
import org.apache.hama.matrix.algebra.TransposeMap;
import org.apache.hama.matrix.algebra.TransposeReduce;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.RandomVariable;
import org.apache.log4j.Logger;

/**
 * Methods of the matrix classes
 */
public abstract class AbstractMatrix implements Matrix {
  static int tryPathLength = Constants.DEFAULT_PATH_LENGTH;
  static final Logger LOG = Logger.getLogger(AbstractMatrix.class);

  protected HamaConfiguration config;
  protected HBaseAdmin admin;
  // a matrix just need a table path to point to the table which stores matrix.
  // let HamaAdmin manage Matrix Name space.
  protected String matrixPath;
  protected HTable table;
  protected HTableDescriptor tableDesc;
  protected HamaAdmin hamaAdmin;

  protected boolean closed = true;

  /**
   * Sets the job configuration
   * 
   * @param conf configuration object
   * @throws MasterNotRunningException
   */
  public void setConfiguration(HamaConfiguration conf)
      throws MasterNotRunningException {
    this.config = conf;
    this.admin = new HBaseAdmin(config);

    hamaAdmin = new HamaAdminImpl(conf, admin);
  }

  /**
   * try to create a new matrix with a new random name. try times will be
   * (Integer.MAX_VALUE - 4) * DEFAULT_TRY_TIMES;
   * 
   * @throws IOException
   */
  protected void tryToCreateTable(String table_prefix) throws IOException {
    int tryTimes = Constants.DEFAULT_TRY_TIMES;
    do {
      matrixPath = table_prefix + "_"
          + RandomVariable.randMatrixPath(tryPathLength);

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
   * Create matrix space
   */
  protected void create() throws IOException {
    // It should run only when table doesn't exist.
    if (!admin.tableExists(matrixPath)) {
      this.tableDesc.addFamily(new HColumnDescriptor(Constants.COLUMNFAMILY));
      this.tableDesc.addFamily(new HColumnDescriptor(Constants.ATTRIBUTE));
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.ALIASEFAMILY)));

      // It's a temporary data.
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.BLOCK)));
      // the following families are used in JacobiEigenValue computation
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.EI)));
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.EICOL)));
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.EIVEC)));

      LOG.info("Initializing the matrix storage.");
      this.admin.createTable(this.tableDesc);
      LOG.info("Create Matrix " + matrixPath);

      // connect to the table.
      table = new HTable(config, matrixPath);
      table.setAutoFlush(true);

      // Record the matrix type in METADATA_TYPE
      Put put = new Put(Bytes.toBytes(Constants.METADATA));
      put.add(Constants.ATTRIBUTE, Bytes.toBytes(Constants.METADATA_TYPE),
          Bytes.toBytes(this.getClass().getSimpleName()));
      table.put(put);

      // the new matrix's reference is 1.
      setReference(1);
    }
  }

  public HTable getHTable() {
    return this.table;
  }

  protected double getNorm1() throws IOException {
    final FileSystem fs = FileSystem.get(config);
    Path outDir = new Path(new Path(getType() + "_TMP_norm1_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(config, "norm1 MR job : " + this.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(this.getPath(), scan,
        MatrixNormMapReduce.MatrixOneNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixOneNormCombiner.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixOneNormReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
      System.out.println(job.reduceProgress());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(config, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  protected double getMaxvalue() throws IOException {
    final FileSystem fs = FileSystem.get(config);
    Path outDir = new Path(new Path(getType() + "_TMP_normMaxValue_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(config, "MaxValue Norm MR job : " + this.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(this.getPath(), scan,
        MatrixNormMapReduce.MatrixMaxValueNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixMaxValueNormReducer.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixMaxValueNormReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(config, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  protected double getInfinity() throws IOException {
    final FileSystem fs = FileSystem.get(config);
    Path outDir = new Path(new Path(getType() + "_TMP_normInifity_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(config, "Infinity Norm MR job : " + this.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(this.getPath(), scan,
        MatrixNormMapReduce.MatrixInfinityNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixInfinityNormReduce.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixInfinityNormReduce.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(config, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  protected double getFrobenius() throws IOException {
    final FileSystem fs = FileSystem.get(config);
    Path outDir = new Path(new Path(getType() + "_TMP_normFrobenius_dir_"
        + System.currentTimeMillis()), "out");
    if (fs.exists(outDir))
      fs.delete(outDir, true);

    Job job = new Job(config, "Frobenius Norm MR job : " + this.getPath());
    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);

    TableMapReduceUtil.initTableMapperJob(this.getPath(), scan,
        MatrixNormMapReduce.MatrixFrobeniusNormMapper.class, IntWritable.class,
        DoubleWritable.class, job);

    job.setCombinerClass(MatrixNormMapReduce.MatrixFrobeniusNormCombiner.class);
    job.setReducerClass(MatrixNormMapReduce.MatrixFrobeniusNormReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    // read outputs
    double result = readOutput(config, fs, outDir);
    fs.delete(outDir.getParent(), true);
    return result;
  }

  private double readOutput(HamaConfiguration config, FileSystem fs, Path outDir)
      throws IOException {
    Path inFile = new Path(outDir, "part-r-00000");
    IntWritable numInside = new IntWritable();
    DoubleWritable result = new DoubleWritable();
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, config);
    try {
      reader.next(numInside, result);
    } finally {
      reader.close();
    }
    return result.get();
  }

  /** {@inheritDoc} */
  public int getRows() throws IOException {
    Get get = new Get(Bytes.toBytes(Constants.METADATA));
    get.addFamily(Constants.ATTRIBUTE);
    byte[] result = table.get(get).getValue(Constants.ATTRIBUTE,
        Bytes.toBytes("rows"));

    return (result != null) ? Bytes.toInt(result) : 0;
  }

  /** {@inheritDoc} */
  public int getColumns() throws IOException {
    Get get = new Get(Bytes.toBytes(Constants.METADATA));
    get.addFamily(Constants.ATTRIBUTE);
    byte[] result = table.get(get).getValue(Constants.ATTRIBUTE,
        Bytes.toBytes("columns"));

    return Bytes.toInt(result);
  }

  /** {@inheritDoc} */
  public String getRowLabel(int row) throws IOException {
    Get get = new Get(BytesUtil.getRowIndex(row));
    get.addFamily(Constants.ATTRIBUTE);
    byte[] result = table.get(get).getValue(Constants.ATTRIBUTE,
        Bytes.toBytes("string"));

    return (result != null) ? Bytes.toString(result) : null;
  }

  /** {@inheritDoc} */
  public void setColumnLabel(int column, String name) throws IOException {
    Put put = new Put(Bytes.toBytes(Constants.CINDEX));
    put.add(Constants.ATTRIBUTE, Bytes.toBytes(String.valueOf(column)), Bytes
        .toBytes(name));
    table.put(put);
  }

  /** {@inheritDoc} */
  public String getColumnLabel(int column) throws IOException {
    Get get = new Get(Bytes.toBytes(Constants.CINDEX));
    get.addFamily(Constants.ATTRIBUTE);
    byte[] result = table.get(get).getValue(Constants.ATTRIBUTE,
        Bytes.toBytes(String.valueOf(column)));

    return (result != null) ? Bytes.toString(result) : null;
  }

  /** {@inheritDoc} */
  public void setRowLabel(int row, String name) throws IOException {
    Put put = new Put(BytesUtil.getRowIndex(row));
    put.add(Constants.ATTRIBUTE, Bytes.toBytes("string"), Bytes
        .toBytes(name));
    table.put(put);
  }

  /** {@inheritDoc} */
  public void setDimension(int rows, int columns) throws IOException {
    Put put = new Put(Bytes.toBytes(Constants.METADATA));
    put.add(Constants.ATTRIBUTE, Bytes.toBytes("rows"), Bytes.toBytes(rows));
    put.add(Constants.ATTRIBUTE, Bytes.toBytes("columns"), Bytes
        .toBytes(columns));
    table.put(put);
  }

  /** {@inheritDoc} */
  public void add(int i, int j, double value) throws IOException {
    Put put = new Put(BytesUtil.getRowIndex(i));
    put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String.valueOf(j)),
        Bytes.toBytes(value + this.get(i, j)));
    table.put(put);

  }

  public static class ScanMapper extends
      TableMapper<ImmutableBytesWritable, Put> implements Configurable {
    private static Double alpha = null;
    private Configuration conf = null;

    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      Put put = new Put(key.get());

      NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = value
          .getNoVersionMap();
      for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> a : map.entrySet()) {
        byte[] family = a.getKey();
        for (Map.Entry<byte[], byte[]> b : a.getValue().entrySet()) {
          byte[] qualifier = b.getKey();
          byte[] val = b.getValue();
          if (alpha.equals(new Double(1))) {
            put.add(family, qualifier, val);
          } else {
            if (Bytes.toString(family).equals(
                Bytes.toString(Constants.COLUMNFAMILY))) {
              double currVal = Bytes.toDouble(val);
              put.add(family, qualifier, Bytes.toBytes(currVal * alpha));
            } else {
              put.add(family, qualifier, val);
            }
          }
        }
      }

      context.write(key, put);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      Float f = conf.getFloat("set.alpha", 1);
      alpha = f.doubleValue();
    }
  }

  /** {@inheritDoc} */
  public Matrix set(Matrix B) throws IOException {
    Job job = new Job(config, "set MR job : " + this.getPath());

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);
    scan.addFamily(Constants.ATTRIBUTE);
    scan.addFamily(Bytes.toBytes(Constants.ALIASEFAMILY));
    scan.addFamily(Bytes.toBytes(Constants.BLOCK));
    scan.addFamily(Bytes.toBytes(Constants.EI));
    scan.addFamily(Bytes.toBytes(Constants.EICOL));
    scan.addFamily(Bytes.toBytes(Constants.EIVEC));

    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableMapperJob(B
        .getPath(), scan, ScanMapper.class, ImmutableBytesWritable.class,
        Put.class, job);
    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableReducerJob(
        this.getPath(),
        org.apache.hadoop.hbase.mapreduce.IdentityTableReducer.class, job);
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return this;
  }

  /** {@inheritDoc} */
  public Matrix set(double alpha, Matrix B) throws IOException {
    Job job = new Job(config, "set MR job : " + this.getPath());

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);
    scan.addFamily(Constants.ATTRIBUTE);
    scan.addFamily(Bytes.toBytes(Constants.ALIASEFAMILY));
    scan.addFamily(Bytes.toBytes(Constants.BLOCK));
    scan.addFamily(Bytes.toBytes(Constants.EI));
    scan.addFamily(Bytes.toBytes(Constants.EICOL));
    scan.addFamily(Bytes.toBytes(Constants.EIVEC));
    Float f = new Float(alpha);
    job.getConfiguration().setFloat("set.alpha", f);

    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableMapperJob(B
        .getPath(), scan, ScanMapper.class, ImmutableBytesWritable.class,
        Put.class, job);
    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableReducerJob(
        this.getPath(),
        org.apache.hadoop.hbase.mapreduce.IdentityTableReducer.class, job);
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return this;
  }

  /** {@inheritDoc} */
  public String getPath() {
    return matrixPath;
  }

  protected void setReference(int reference) throws IOException {
    Put put = new Put(Bytes.toBytes(Constants.METADATA));
    put.add(Constants.ATTRIBUTE, Bytes.toBytes(Constants.METADATA_REFERENCE),
        Bytes.toBytes(reference));
    table.put(put);
  }

  protected int incrementAndGetRef() throws IOException {
    int reference = 1;

    Get get = new Get(Bytes.toBytes(Constants.METADATA));
    get.addFamily(Constants.ATTRIBUTE);
    byte[] result = table.get(get).getValue(Constants.ATTRIBUTE,
        Bytes.toBytes(Constants.METADATA_REFERENCE));

    if (result != null) {
      reference = Bytes.toInt(result);
      reference++;
    }
    setReference(reference);
    return reference;
  }

  protected int decrementAndGetRef() throws IOException {
    int reference = 0;

    Get get = new Get(Bytes.toBytes(Constants.METADATA));
    get.addFamily(Constants.ATTRIBUTE);
    byte[] result = table.get(get).getValue(Constants.ATTRIBUTE,
        Bytes.toBytes(Constants.METADATA_REFERENCE));

    if (result != null) {
      reference = Bytes.toInt(result);
      if (reference > 0) // reference==0, we need not to decrement it.
        reference--;
    }
    setReference(reference);
    return reference;
  }

  protected boolean hasAliaseName() throws IOException {
    Get get = new Get(Bytes.toBytes(Constants.METADATA));
    get.addFamily(Bytes.toBytes(Constants.ALIASEFAMILY));
    byte[] result = table.get(get).getValue(
        Bytes.toBytes(Constants.ALIASEFAMILY), Bytes.toBytes("name"));

    return (result != null) ? true : false;
  }

  public void close() throws IOException {
    if (closed) // have been closed
      return;
    int reference = decrementAndGetRef();
    if (reference <= 0) { // no reference again.
      if (!hasAliaseName()) { // the table has not been aliased, we delete the
        // table.
        if (admin.isTableEnabled(matrixPath)) {
          while (admin.isTableEnabled(matrixPath)) {
            try {
              admin.disableTable(matrixPath);
            } catch (RegionException e) {
              LOG.warn(e);
            }
          }

          admin.deleteTable(matrixPath);
        }
      }
    }
    closed = true;
  }

  public Matrix transpose() throws IOException {
    Matrix result;
    if (this.getType().equals("SparseMatrix")) {
      result = new SparseMatrix(config, this.getRows(), this.getColumns());
    } else {
      result = new DenseMatrix(config, this.getRows(), this.getColumns());
    }

    Job job = new Job(config, "set MR job : " + this.getPath());

    Scan scan = new Scan();
    scan.addFamily(Constants.COLUMNFAMILY);
    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableMapperJob(
        this.getPath(), scan, TransposeMap.class, IntWritable.class,
        MapWritable.class, job);
    org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.initTableReducerJob(
        result.getPath(), TransposeReduce.class, job);
    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    return result;
  }

  public boolean checkAllJobs(List<Job> jobId) throws IOException {
    Iterator<Job> it = jobId.iterator();
    boolean allTrue = true;
    while (it.hasNext()) {
      if (!it.next().isComplete()) {
        allTrue = false;
      }
    }

    return allTrue;
  }

  public boolean save(String aliasename) throws IOException {
    // mark & update the aliase name in "alise:name" meta column.
    // ! one matrix has only one aliasename now.
    Put put = new Put(Bytes.toBytes(Constants.METADATA));
    put.add(Bytes.toBytes(Constants.ALIASEFAMILY), Bytes.toBytes("name"), Bytes
        .toBytes(aliasename));
    put.add(Constants.ATTRIBUTE, Bytes.toBytes("type"), Bytes.toBytes(this
        .getType()));
    table.put(put);

    return hamaAdmin.save(this, aliasename);
  }
}
