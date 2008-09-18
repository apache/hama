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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.Numeric;
import org.apache.log4j.Logger;

/**
 * Methods of the matrix classes
 */
public abstract class AbstractMatrix implements Matrix {
  static final Logger LOG = Logger.getLogger(AbstractMatrix.class);

  /** Hama Configuration */
  protected HamaConfiguration config;
  /** Hbase admin object */
  protected HBaseAdmin admin;
  /** The name of Matrix */
  protected String matrixName;
  /** Hbase table object */
  protected HTable table;
  /** Matrix attribute description */
  protected HTableDescriptor tableDesc;
  public HamaAdmin hAdmin;
  
  /**
   * Sets the job configuration
   * 
   * @param conf configuration object
   */
  public void setConfiguration(HamaConfiguration conf) {
    this.config = conf;
    try {
      this.admin = new HBaseAdmin(config);
    } catch (MasterNotRunningException e) {
      LOG.error(e, e);
    }
    
    hAdmin = new HamaAdmin(conf, admin);
  }

  /**
   * Create matrix space
   */
  protected void create() throws IOException {
    this.tableDesc.addFamily(new HColumnDescriptor(Constants.COLUMN));
    this.tableDesc.addFamily(new HColumnDescriptor(Constants.ATTRIBUTE));
    
    LOG.info("Initializing the matrix storage.");
    this.admin.createTable(this.tableDesc);
  }

  public void execute(JobConf jobConf, Matrix result) throws IOException {
    RunningJob rJob = JobClient.runJob(jobConf);
    // TODO : When HADOOP-4043 done, we should change this.
    long rows = rJob.getCounters().findCounter(
        "org.apache.hadoop.mapred.Task$Counter", 8, "REDUCE_OUTPUT_RECORDS")
        .getCounter();
    // TODO : Thinking about more efficient method.
    int columns = result.getRow(0).size();
    result.setDimension((int) rows, columns);
  }

  /** {@inheritDoc} */
  public double get(int i, int j) throws IOException {
    double result = -1;
    Cell c = table.get(Numeric.intToBytes(i), Numeric.getColumnIndex(j));
    if (c != null) {
      result = Numeric.bytesToDouble(c.getValue());
    }
    return result;
  }

  /** {@inheritDoc} */
  public int getRows() throws IOException {
    Cell rows = null;
    rows = table.get(Constants.METADATA, Constants.METADATA_ROWS);
    return Numeric.bytesToInt(rows.getValue());
  }

  /** {@inheritDoc} */
  public int getColumns() throws IOException {
    Cell columns = table.get(Constants.METADATA, Constants.METADATA_COLUMNS);
    return Numeric.bytesToInt(columns.getValue());
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double value) throws IOException {
    VectorUpdate update = new VectorUpdate(i);
    update.put(j, value);
    table.commit(update.getBatchUpdate());
  }

  /** {@inheritDoc} */
  public void add(int i, int j, double value) throws IOException {
    // TODO Auto-generated method stub
  }

  /** {@inheritDoc} */
  public void setDimension(int rows, int columns) throws IOException {
    VectorUpdate update = new VectorUpdate(Constants.METADATA);
    update.put(Constants.METADATA_ROWS, rows);
    update.put(Constants.METADATA_COLUMNS, columns);

    table.commit(update.getBatchUpdate());
  }

  public String getRowAttribute(int row) throws IOException {
    Cell rows = null;
    rows = table.get(Numeric.intToBytes(row), Bytes.toBytes(Constants.ATTRIBUTE
        + "string"));

    return (rows != null) ? Bytes.toString(rows.getValue()) : null;
  }

  public void setRowAttribute(int row, String name) throws IOException {
    VectorUpdate update = new VectorUpdate(row);
    update.put(Constants.ATTRIBUTE + "string", name);
    table.commit(update.getBatchUpdate());
  }

  public String getColumnAttribute(int column) throws IOException {
    Cell rows = null;
    rows = table.get(Constants.CINDEX, 
        (Constants.ATTRIBUTE + column));
    return (rows != null) ? Bytes.toString(rows.getValue()) : null;
  }

  public void setColumnAttribute(int column, String name) throws IOException {
    VectorUpdate update = new VectorUpdate(Constants.CINDEX);
    update.put(column, name);
    table.commit(update.getBatchUpdate());
  }

  /** {@inheritDoc} */
  public String getName() {
    return (matrixName != null) ? matrixName : null;
  }
  
  public void clear() throws IOException {
    admin.deleteTable(matrixName);
  }

  public boolean save(String path) throws IOException {
    return hAdmin.put(this.matrixName, path);
  }
}
