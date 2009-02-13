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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.IdentityTableReduce;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.BytesUtil;
import org.apache.hama.util.JobManager;
import org.apache.log4j.Logger;

/**
 * Methods of the matrix classes
 */
public abstract class AbstractMatrix implements Matrix {
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
   * Create matrix space
   */
  protected void create() throws IOException {
    // It should run only when table doesn't exist.
    if (!admin.tableExists(matrixPath)) {
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.COLUMN), 3, CompressionType.NONE, false, false,
          Integer.MAX_VALUE, HConstants.FOREVER, false));
      this.tableDesc.addFamily(new HColumnDescriptor(Constants.ATTRIBUTE));
      this.tableDesc.addFamily(new HColumnDescriptor(Constants.ALIASEFAMILY));
      // It's a temporary data.
      this.tableDesc.addFamily(new HColumnDescriptor(Bytes
          .toBytes(Constants.BLOCK), 1, CompressionType.NONE, false, false,
          Integer.MAX_VALUE, HConstants.FOREVER, false));

      LOG.info("Initializing the matrix storage.");
      this.admin.createTable(this.tableDesc);
      LOG.info("Create Matrix " + matrixPath);

      // connect to the table.
      table = new HTable(config, matrixPath);
      table.setAutoFlush(true);

      // Record the matrix type in METADATA_TYPE
      BatchUpdate update = new BatchUpdate(Constants.METADATA);
      update.put(Constants.METADATA_TYPE, Bytes.toBytes(this.getClass()
          .getSimpleName()));

      table.commit(update);

      // the new matrix's reference is 1.
      setReference(1);
    }
  }

  public HTable getHTable() {
    return this.table;
  }

  /** {@inheritDoc} */
  public int getRows() throws IOException {
    Cell rows = null;
    rows = table.get(Constants.METADATA, Constants.METADATA_ROWS);
    return (rows != null) ? BytesUtil.bytesToInt(rows.getValue()) : 0;
  }

  /** {@inheritDoc} */
  public int getColumns() throws IOException {
    Cell columns = table.get(Constants.METADATA, Constants.METADATA_COLUMNS);
    return BytesUtil.bytesToInt(columns.getValue());
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double value) throws IOException {
    VectorUpdate update = new VectorUpdate(i);
    update.put(j, value);
    table.commit(update.getBatchUpdate());

  }

  /** {@inheritDoc} */
  public void add(int i, int j, double value) throws IOException {
    VectorUpdate update = new VectorUpdate(i);
    update.put(j, value + this.get(i, j));
    table.commit(update.getBatchUpdate());

  }

  /** {@inheritDoc} */
  public void setDimension(int rows, int columns) throws IOException {
    VectorUpdate update = new VectorUpdate(Constants.METADATA);
    update.put(Constants.METADATA_ROWS, rows);
    update.put(Constants.METADATA_COLUMNS, columns);

    table.commit(update.getBatchUpdate());
  }

  /**
   * Just full scan a table.
   */
  public static class TableReadMapper extends MapReduceBase implements
      TableMap<ImmutableBytesWritable, BatchUpdate> {
    private static List<Double> alpha = new ArrayList<Double>();

    @SuppressWarnings("unchecked")
    public void map(ImmutableBytesWritable key, RowResult value,
        OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
        @SuppressWarnings("unused")
        Reporter reporter) throws IOException {

      BatchUpdate update = new BatchUpdate(key.get());
      for (Map.Entry<byte[], Cell> e : value.entrySet()) {
        if (alpha.size() == 0) {
          update.put(e.getKey(), e.getValue().getValue());
        } else {
          String column = new String(e.getKey());
          if(column.startsWith(Constants.COLUMN)) {
            double currValue = BytesUtil.bytesToDouble(e.getValue().getValue());
            update.put(e.getKey(), (BytesUtil.doubleToBytes(currValue * alpha.get(0))));
          } else {
            update.put(e.getKey(), e.getValue().getValue());
          }
        }
      }
      output.collect(key, update);
    }

    public static void setAlpha(double a) {
      if(alpha.size() > 0) 
        alpha = new ArrayList<Double>();
      alpha.add(a);
    }
  }

  /** {@inheritDoc} */
  public Matrix set(Matrix B) throws IOException {
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("set MR job : " + this.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    TableMapReduceUtil.initTableMapJob(B.getPath(), Constants.COLUMN + " "
        + Constants.ATTRIBUTE + " " + Constants.ALIASEFAMILY + " "
        + Constants.BLOCK, TableReadMapper.class, ImmutableBytesWritable.class,
        BatchUpdate.class, jobConf);
    TableMapReduceUtil.initTableReduceJob(this.getPath(),
        IdentityTableReduce.class, jobConf);

    JobManager.execute(jobConf);
    return this;
  }

  /** {@inheritDoc} */
  public Matrix set(double alpha, Matrix B) throws IOException {
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("set MR job : " + this.getPath());

    jobConf.setNumMapTasks(config.getNumMapTasks());
    jobConf.setNumReduceTasks(config.getNumReduceTasks());

    TableReadMapper.setAlpha(alpha);
    TableMapReduceUtil.initTableMapJob(B.getPath(), Constants.COLUMN + " "
        + Constants.ATTRIBUTE + " " + Constants.ALIASEFAMILY + " "
        + Constants.BLOCK, TableReadMapper.class, ImmutableBytesWritable.class,
        BatchUpdate.class, jobConf);
    TableMapReduceUtil.initTableReduceJob(this.getPath(),
        IdentityTableReduce.class, jobConf);

    JobManager.execute(jobConf);
    return this;
  }

  public String getRowLabel(int row) throws IOException {
    Cell rows = null;
    rows = table.get(BytesUtil.getRowIndex(row), Bytes
        .toBytes(Constants.ATTRIBUTE + "string"));

    return (rows != null) ? Bytes.toString(rows.getValue()) : null;
  }

  public void setRowLabel(int row, String name) throws IOException {
    VectorUpdate update = new VectorUpdate(row);
    update.put(Constants.ATTRIBUTE + "string", name);
    table.commit(update.getBatchUpdate());

  }

  public String getColumnLabel(int column) throws IOException {
    Cell rows = null;
    rows = table.get(Constants.CINDEX, (Constants.ATTRIBUTE + column));
    return (rows != null) ? Bytes.toString(rows.getValue()) : null;
  }

  public void setColumnLabel(int column, String name) throws IOException {
    VectorUpdate update = new VectorUpdate(Constants.CINDEX);
    update.put(column, name);
    table.commit(update.getBatchUpdate());
  }

  /** {@inheritDoc} */
  public String getPath() {
    return matrixPath;
  }

  protected void setReference(int reference) throws IOException {
    BatchUpdate update = new BatchUpdate(Constants.METADATA);
    update.put(Constants.METADATA_REFERENCE, Bytes.toBytes(reference));
    table.commit(update);

  }

  protected int incrementAndGetRef() throws IOException {
    int reference = 1;
    Cell rows = null;
    rows = table.get(Constants.METADATA, Constants.METADATA_REFERENCE);
    if (rows != null) {
      reference = Bytes.toInt(rows.getValue());
      reference++;
    }
    setReference(reference);
    return reference;
  }

  protected int decrementAndGetRef() throws IOException {
    int reference = 0;
    Cell rows = null;
    rows = table.get(Constants.METADATA, Constants.METADATA_REFERENCE);
    if (rows != null) {
      reference = Bytes.toInt(rows.getValue());
      if (reference > 0) // reference==0, we need not to decrement it.
        reference--;
    }
    setReference(reference);
    return reference;
  }

  protected boolean hasAliaseName() throws IOException {
    Cell rows = null;
    rows = table.get(Constants.METADATA, Constants.ALIASENAME);
    return (rows != null) ? true : false;
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

  public boolean save(String aliasename) throws IOException {
    // mark & update the aliase name in "alise:name" meta column.
    // ! one matrix has only one aliasename now.
    BatchUpdate update = new BatchUpdate(Constants.METADATA);
    update.put(Constants.ALIASENAME, Bytes.toBytes(aliasename));
    table.commit(update);

    return hamaAdmin.save(this, aliasename);
  }
}
