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
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.Text;
import org.apache.hama.mapred.MatrixMapReduce;
import org.apache.log4j.Logger;

/**
 * Methods of the matrix classes
 */
public abstract class AbstractMatrix extends MatrixMapReduce implements
    MatrixInterface {
  static final Logger LOG = Logger.getLogger(AbstractMatrix.class);

  /** Hbase Configuration */
  protected HBaseConfiguration config;
  /** Hbase admin object */
  protected HBaseAdmin admin;
  /** The name of Matrix */
  protected Text matrixName;
  /** Hbase table object */
  protected HTable table;
  /** Matrix attribute description */
  protected HTableDescriptor tableDesc;
  /** The parallel degree of map function */
  protected int mapper;
  /** The parallel degree of reduce function */
  protected int reducer;

  /**
   * Sets the job configuration
   * 
   * @param conf configuration object
   */
  public void setConfiguration(Configuration conf) {
    config = (HBaseConfiguration) conf;
    try {
      admin = new HBaseAdmin(config);
    } catch (MasterNotRunningException e) {
      LOG.info(e);
    }
    mapper = conf.getInt("mapred.map.tasks", 1);
    reducer = conf.getInt("mapred.reduce.tasks", 1);
  }

  /**
   * Create matrix space
   */
  protected void create() {
    try {
      tableDesc.addFamily(new HColumnDescriptor(Constants.METADATA.toString()));
      LOG.info("Initializaing.");
      admin.createTable(tableDesc);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /** {@inheritDoc} */
  public int getRowDimension() {
    Cell rows = null;
    try {
      rows = table.get(Constants.METADATA, Constants.METADATA_ROWS);
    } catch (IOException e) {
      LOG.error(e, e);
    }

    return bytesToInt(rows.getValue());
  }

  /** {@inheritDoc} */
  public int getColumnDimension() {
    Cell columns = null;
    try {
      columns = table.get(Constants.METADATA, Constants.METADATA_COLUMNS);
    } catch (IOException e) {
      LOG.error(e, e);
    }
    return bytesToInt(columns.getValue());
  }

  /** {@inheritDoc} */
  public double get(int i, int j) {
    Text row = new Text(String.valueOf(i));
    Text column = new Text(Constants.COLUMN + String.valueOf(j));
    Cell c;
    double result = -1;
    try {
      c = table.get(row, column);
      if (c != null) {
        result = toDouble(c.getValue());
      }
    } catch (IOException e) {
      LOG.error(e, e);
    }
    return result;
  }

  /** {@inheritDoc} */
  public FeatureVector getRowVector(int row) {
    try {
      SortedMap<Integer, Double> result = new TreeMap<Integer, Double>();
      for (Map.Entry<Text, Cell> f : table
          .getRow(new Text(String.valueOf(row))).entrySet()) {
        result.put(getIndex(f.getKey()), toDouble(f.getValue().getValue()));
      }
      return new FeatureVector(result);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /** {@inheritDoc} */
  public void set(int i, int j, double d) {
    BatchUpdate b = new BatchUpdate(new Text(String.valueOf(i)));
    b.put(new Text(Constants.COLUMN + String.valueOf(j)), toBytes(d));
    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /** {@inheritDoc} */
  public void add(int i, int j, double d) {
    // TODO Auto-generated method stub
  }

  /** {@inheritDoc} */
  public void deleteColumnEquals(int j) {
    // TODO Auto-generated method stub
  }

  /** {@inheritDoc} */
  public void deleteRowEquals(int i) {
    // TODO Auto-generated method stub
  }

  /** {@inheritDoc} */
  public void reset(int m, int n) {
    // TODO Auto-generated method stub
  }

  /** {@inheritDoc} */
  public void setDimension(int rows, int columns) {
    BatchUpdate b = new BatchUpdate(Constants.METADATA);
    b.put(Constants.METADATA_ROWS, intToBytes(rows));
    b.put(Constants.METADATA_COLUMNS, intToBytes(columns));

    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /** {@inheritDoc} */
  public String getName() {
    return (matrixName != null) ? matrixName.toString() : null;
  }

  /**
   * Return the value of determinant
   * 
   * @return the value of determinant
   */
  public double getDeterminant() {
    try {
      return toDouble(table.get(
          new Text(String.valueOf(Constants.DETERMINANT)),
          new Text(Constants.COLUMN)).getValue());
    } catch (IOException e) {
      LOG.error(e, e);
      return -1;
    }
  }

  /** {@inheritDoc} */
  public Matrix copy() {
    // TODO
    return null;
  }

  /** {@inheritDoc} */
  public void save(String matrixName) {
    // TODO
  }

  /** {@inheritDoc} */
  public void close() {
    admin = null;
    matrixName = null;
    tableDesc = null;
  }

  /** {@inheritDoc} */
  public void clear() {
    try {
      admin.deleteTable(matrixName);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }
}
