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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.Text;

/**
 * A library for mathematical operations on matrices of triangle forms
 */
public class TriangularMatrix extends AbstractMatrix {

  /**
   * Construct
   * 
   * @param conf configuration object
   */
  public TriangularMatrix(Configuration conf) {
    setConfiguration(conf);
  }

  /**
   * Construct an triangular matrix
   * 
   * @param conf configuration object
   * @param matrixName the name of the triangular matrix
   */
  public TriangularMatrix(HBaseConfiguration conf, Text matrixName) {
    try {
      setConfiguration(conf);
      this.matrixName = matrixName;

      if (!admin.tableExists(matrixName)) {
        tableDesc = new HTableDescriptor(matrixName.toString());
        tableDesc.addFamily(new HColumnDescriptor(Constants.LOWER.toString()));
        tableDesc.addFamily(new HColumnDescriptor(Constants.UPPER.toString()));
        create();
      }

      table = new HTable(conf, matrixName);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** {@inheritDoc} */
  public Matrix multiply(Matrix b) {
    return null;
  }

  /** {@inheritDoc} */
  public Matrix addition(Matrix b) {
    return null;
  }

  /** {@inheritDoc} */
  public double determinant() {
    return 0;
  }

  /** {@inheritDoc} */
  public Matrix subtraction(Matrix b) {
    return null;
  }

  /** {@inheritDoc} */
  public TriangularMatrix decompose(Decomposition technique) {
    return null;
  }

  /**
   * Gets the entry value of lower matrix L(i, j)
   * 
   * @param i ith row of lower matrix L
   * @param j jth column of lower matrix L
   * @return the value of entry
   */
  public double getFromLower(int i, int j) {
    // If row doesn't exist, return the null pointer exception.
    //So, we need to make a Cell object. -- Edward
    
    Text row = new Text(String.valueOf(i));
    Text column = new Text(Constants.LOWER + String.valueOf(j));

    try {
      Cell c = table.get(row, column);
      if (c == null) {
        return 0;
      } else {
        return toDouble(c.getValue());
      }
    } catch (IOException e) {
      LOG.error(e, e);
      return 0;
    }
  }

  /**
   * Gets the entry value of upper matrix U(i, j)
   * 
   * @param i ith row of upper matrix U
   * @param j jth column of upper matrix U
   * @return the value of entry
   */
  public double getFromUpper(int i, int j) {
    try {
      return toDouble(table.get(new Text(String.valueOf(i)),
          new Text(Constants.UPPER + String.valueOf(j))).getValue());
    } catch (IOException e) {
      LOG.error(e, e);
      return 0;
    }
  }

  /**
   * Sets the double value of lower matrix L(i, j)
   * 
   * @param i ith row of lower matrix L
   * @param j jth column of lower matrix L
   * @param d the value of entry
   */
  public void setToLower(int i, int j, double d) {
    BatchUpdate b = new BatchUpdate(new Text(String.valueOf(i)));
    b.put(new Text(Constants.LOWER + String.valueOf(j)), toBytes(d));
    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /**
   * Sets the double value of upper matrix U(i, j)
   * 
   * @param i ith row of upper matrix U
   * @param j jth column of upper matrix U
   * @param d the value of entry
   */
  public void setToUpper(int i, int j, double d) {
    BatchUpdate b = new BatchUpdate(new Text(String.valueOf(i)));
    b.put(new Text(Constants.UPPER + String.valueOf(j)), toBytes(d));
    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }
}
