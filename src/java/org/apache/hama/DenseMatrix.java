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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.algebra.Add1DLayoutMap;
import org.apache.hama.algebra.Add1DLayoutReduce;
import org.apache.hama.algebra.Mult1DLayoutMap;
import org.apache.hama.algebra.Mult1DLayoutReduce;
import org.apache.hama.io.VectorEntry;
import org.apache.hama.io.VectorMapWritable;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.mapred.MatrixReduce;
import org.apache.hama.util.JobManager;
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
   * @throws IOException
   */
  public static Matrix random(HamaConfiguration conf, int m, int n)
      throws IOException {
    String name = RandomVariable.randMatrixName();
    Matrix rand = new DenseMatrix(conf, name);
    for (int i = 0; i < m; i++) {
      DenseVector vector = new DenseVector();
      for (int j = 0; j < n; j++) {
        vector.set(j, RandomVariable.rand());
      }
      rand.setRow(i, vector);
    }

    rand.setDimension(m, n);
    LOG.info("Create the " + m + " * " + n + " random matrix : " + name);
    return rand;
  }

  public Matrix add(Matrix B) throws IOException {
    String output = RandomVariable.randMatrixName();
    Matrix result = new DenseMatrix(config, output);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("addition MR job" + result.getName());

    jobConf.setNumMapTasks(Integer.parseInt(config.get("mapred.map.tasks")));
    jobConf.setNumReduceTasks(Integer.parseInt(config
        .get("mapred.reduce.tasks")));

    Add1DLayoutMap.initJob(this.getName(), B.getName(), Add1DLayoutMap.class,
        IntWritable.class, VectorWritable.class, jobConf);
    MatrixReduce.initJob(result.getName(), Add1DLayoutReduce.class, jobConf);

    JobManager.execute(jobConf, result);
    return result;
  }

  public Matrix add(double alpha, Matrix B) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  public DenseVector getRow(int row) throws IOException {
    VectorMapWritable<Integer, VectorEntry> values = new VectorMapWritable<Integer, VectorEntry>();
    RowResult rowResult = table.getRow(String.valueOf(row));

    for (Map.Entry<byte[], Cell> f : rowResult.entrySet()) {
      VectorEntry entry = new VectorEntry(f.getValue());
      values.put(Numeric.getColumnIndex(f.getKey()), entry);
    }

    return new DenseVector(values);
  }

  public Vector getColumn(int column) throws IOException {
    byte[] columnKey = Numeric.getColumnIndex(column);
    byte[][] c = { columnKey };
    Scanner scan = table.getScanner(c, HConstants.EMPTY_START_ROW);

    VectorMapWritable<Integer, VectorEntry> trunk = new VectorMapWritable<Integer, VectorEntry>();

    for (RowResult row : scan) {
      trunk.put(Numeric.bytesToInt(row.getRow()), new VectorEntry(row
          .get(columnKey)));
    }

    return new DenseVector(trunk);
  }

  public Matrix mult(Matrix B) throws IOException {
    String output = RandomVariable.randMatrixName();
    Matrix result = new DenseMatrix(config, output);

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("multiplication MR job : " + result.getName());

    jobConf.setNumMapTasks(Integer.parseInt(config.get("mapred.map.tasks")));
    jobConf.setNumReduceTasks(Integer.parseInt(config
        .get("mapred.reduce.tasks")));

    Mult1DLayoutMap.initJob(this.getName(), B.getName(), Mult1DLayoutMap.class,
        IntWritable.class, VectorWritable.class, jobConf);
    MatrixReduce.initJob(result.getName(), Mult1DLayoutReduce.class, jobConf);
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
    // TODO Auto-generated method stub
  }

  public void load(String path) throws IOException {
    matrixName = hAdmin.get(path);
    table = new HTable(matrixName);
  }
}
