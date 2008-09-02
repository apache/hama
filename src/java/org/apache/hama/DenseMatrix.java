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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.algebra.AdditionMap;
import org.apache.hama.algebra.AdditionReduce;
import org.apache.hama.algebra.MultiplicationMap;
import org.apache.hama.algebra.MultiplicationReduce;
import org.apache.hama.mapred.DenseMap;
import org.apache.hama.mapred.MatrixReduce;
import org.apache.hama.util.Numeric;
import org.apache.hama.util.RandomVariable;

public class DenseMatrix extends AbstractMatrix implements Matrix {

  /**
   * Construct
   * 
   * @param conf configuration object
   */
  public DenseMatrix(HamaConfiguration conf) {
    setConfiguration(conf);
  }

  /**
   * Construct an matrix
   * 
   * @param conf configuration object
   * @param matrixName the name of the matrix
   */
  public DenseMatrix(HamaConfiguration conf, String matrixName) {
    try {
      setConfiguration(conf);
      this.matrixName = matrixName;

      if (!admin.tableExists(matrixName)) {
        tableDesc = new HTableDescriptor(matrixName);
        tableDesc.addFamily(new HColumnDescriptor(Constants.COLUMN));
        create();
      }

      table = new HTable(config, matrixName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Construct an m-by-n constant matrix.
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @param s fill the matrix with this scalar value.
   */
  public DenseMatrix(HamaConfiguration conf, int m, int n, double s) {
    try {
      setConfiguration(conf);
      matrixName = RandomVariable.randMatrixName();

      if (!admin.tableExists(matrixName)) {
        tableDesc = new HTableDescriptor(matrixName);
        tableDesc.addFamily(new HColumnDescriptor(Constants.COLUMN));
        create();
      }

      table = new HTable(config, matrixName);

      for (int i = 0; i < m; i++) {
        for (int j = 0; j < n; j++) {
          set(i, j, s);
        }
      }

      setDimension(m, n);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Generate matrix with random elements
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @return an m-by-n matrix with uniformly distributed random elements.
   */
  public static Matrix random(HamaConfiguration conf, int m, int n) {
    String name = RandomVariable.randMatrixName();
    Matrix rand = new DenseMatrix(conf, name);
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        rand.set(i, j, RandomVariable.rand());
      }
    }

    rand.setDimension(m, n);
    LOG.info("Create the " + m + " * " + n + " random matrix : " + name);
    return rand;
  }

  public Matrix add(Matrix B) {
    String output = RandomVariable.randMatrixName();
    Matrix C = new DenseMatrix(config, output);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job");

    DenseMap.initJob(this.getName(), B.getName(), AdditionMap.class,
        IntWritable.class, DenseVector.class, jobConf);
    MatrixReduce.initJob(C.getName(), AdditionReduce.class, jobConf);

    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return C;
  }

  public Matrix add(double alpha, Matrix B) {
    // TODO Auto-generated method stub
    return null;
  }

  public DenseVector getRow(int row) throws IOException {
    return new DenseVector(row, table.getRow(String.valueOf(row)));
  }
  
  public Vector getColumn(int column) throws IOException {
    byte[] columnKey = Numeric.getColumnIndex(column);
    byte[][] c = { columnKey };
    Scanner scan = table.getScanner(c, HConstants.EMPTY_START_ROW);

    HbaseMapWritable<byte[], Cell> trunk = new HbaseMapWritable<byte[], Cell>();

    for (RowResult row : scan) {
      trunk.put(row.getRow(), row.get(columnKey));
    }

    return new DenseVector(columnKey, trunk);
  }

  public Matrix mult(Matrix B) {
    String output = RandomVariable.randMatrixName();
    Matrix C = new DenseMatrix(config, output);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job");

    DenseMap.initJob(this.getName(), B.getName(), MultiplicationMap.class,
        IntWritable.class, DenseVector.class, jobConf);
    MatrixReduce.initJob(C.getName(), MultiplicationReduce.class, jobConf);

    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return C;
  }

  public Matrix multAdd(double alpha, Matrix B, Matrix C) {
    // TODO Auto-generated method stub
    return null;
  }

  public double norm(Norm type) {
    // TODO Auto-generated method stub
    return 0;
  }

  public Matrix set(double alpha, Matrix B) {
    // TODO Auto-generated method stub
    return null;
  }

  public Matrix set(Matrix B) {
    // TODO Auto-generated method stub
    return null;
  }
}
