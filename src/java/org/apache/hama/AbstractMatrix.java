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
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
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

  /**
   * Sets the job configuration
   * 
   * @param conf configuration object
   */
  public void setConfiguration(HamaConfiguration conf) {
    config = (HamaConfiguration) conf;
    try {
      admin = new HBaseAdmin(config);
    } catch (MasterNotRunningException e) {
      LOG.error(e, e);
    }
  }

  /**
   * Create matrix space
   */
  protected void create() {
    try {
      tableDesc.addFamily(new HColumnDescriptor(Constants.METADATA));
      LOG.info("Initializing the matrix storage.");
      admin.createTable(tableDesc);
    } catch (IOException e) {
      LOG.error(e, e);
    }
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
    BatchUpdate b = new BatchUpdate(Numeric.intToBytes(i));
    b.put(Numeric.getColumnIndex(j), Numeric.doubleToBytes(value));
    table.commit(b);
  }

  /** {@inheritDoc} */
  public void add(int i, int j, double value) throws IOException {
    // TODO Auto-generated method stub
  }

  /** {@inheritDoc} */
  public void setDimension(int rows, int columns) throws IOException {
    BatchUpdate b = new BatchUpdate(Constants.METADATA);
    b.put(Constants.METADATA_ROWS, Numeric.intToBytes(rows));
    b.put(Constants.METADATA_COLUMNS, Numeric.intToBytes(columns));

    table.commit(b);
  }

  /** {@inheritDoc} */
  public String getName() {
    return (matrixName != null) ? matrixName : null;
  }
}
