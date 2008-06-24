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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.algebra.AdditionSubtractionMap;
import org.apache.hama.algebra.AdditionSubtractionReduce;
import org.apache.hama.algebra.CholeskyDecompositionMap;
import org.apache.hama.algebra.CroutDecompositionMap;
import org.apache.hama.algebra.DeterminantMap;
import org.apache.hama.algebra.DeterminantReduce;
import org.apache.hama.algebra.MultiplicationMap;
import org.apache.hama.algebra.MultiplicationReduce;
import org.apache.hama.mapred.MatrixInputFormat;
import org.apache.hama.mapred.MatrixOutputFormat;

/**
 * A library for mathematical operations on matrices of double.
 */
public class Matrix extends AbstractMatrix {

  /**
   * Construct
   * 
   * @param conf configuration object
   */
  public Matrix(Configuration conf) {
    setConfiguration(conf);
  }

  /**
   * Construct an matrix
   * 
   * @param conf configuration object
   * @param matrixName the name of the matrix
   */
  public Matrix(Configuration conf, Text matrixName) {
    try {
      setConfiguration(conf);
      this.matrixName = matrixName;

      if (!admin.tableExists(matrixName)) {
        tableDesc = new HTableDescriptor(matrixName.toString());
        tableDesc.addFamily(new HColumnDescriptor(Constants.COLUMN.toString()));
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
  public Matrix(HBaseConfiguration conf, int m, int n, double s) {
    try {
      setConfiguration(conf);
      matrixName = new Text(Constants.RANDOM + System.currentTimeMillis());

      if (!admin.tableExists(matrixName)) {
        tableDesc = new HTableDescriptor(matrixName.toString());
        tableDesc.addFamily(new HColumnDescriptor(Constants.COLUMN.toString()));
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
  public static Matrix random(Configuration conf, int m, int n) {
    Text name = new Text(Constants.RANDOM + System.currentTimeMillis());
    Matrix rand = new Matrix(conf, name);
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        rand.set(i, j, RandomVariable.rand());
      }
    }

    rand.setDimension(m, n);

    return rand;
  }

  /**
   * Generate matrix with identity elements
   * 
   * @param conf configuration object
   * @param m the number of rows.
   * @param n the number of columns.
   * @return an m-by-n matrix with ones on the diagonal and zeros elsewhere.
   */
  public static Matrix identity(HBaseConfiguration conf, int m, int n) {
    // TODO
    return null;
  }

  /** {@inheritDoc} */
  public Matrix multiply(Matrix b) {
    String output = Constants.RESULT + System.currentTimeMillis();
    Matrix c = new Matrix(config, new Text(output));

    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("parallel matrix multiplication of " + getName() + " and " + b.getName());
    jobConf.setInputFormat(MatrixInputFormat.class);
    jobConf.setOutputFormat(MatrixOutputFormat.class);
    MultiplicationMap.initJob(getName(), b.getName(), MultiplicationMap.class, jobConf);
    MultiplicationReduce.initJob(output, MultiplicationReduce.class, jobConf);
    
    jobConf.setNumMapTasks(mapper);
    jobConf.setNumReduceTasks(reducer);
    
    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      LOG.info(e);
    }

    return c;
  }

  /** {@inheritDoc} */
  public Matrix addition(Matrix b) {
    return additionSubtraction(b, Constants.PLUS);
  }

  /** {@inheritDoc} */
  public Matrix subtraction(Matrix b) {
    return additionSubtraction(b, Constants.PLUS);
  }

  /**
   * Method for add or subtract operation 
   * 
   * @param target
   * @param operator
   * @return matrix
   */  
  public Matrix additionSubtraction(Matrix target, String operator) {
    String b = target.getName();
    String output = Constants.RESULT + System.currentTimeMillis();
    Matrix c = new Matrix(config, new Text(output));
    String jobName = "parallel matrix " + operator + " of " + getName() + " and " + b;
    LOG.info(jobName);
    
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName(jobName);
    jobConf.setInputFormat(MatrixInputFormat.class);
    jobConf.setOutputFormat(MatrixOutputFormat.class);
    AdditionSubtractionMap.initJob(getName(), b, operator,
        AdditionSubtractionMap.class, jobConf);
    AdditionSubtractionReduce.initJob(output,
        AdditionSubtractionReduce.class, jobConf);
    
    jobConf.setNumMapTasks(mapper);
    jobConf.setNumReduceTasks(reducer);
    
    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      LOG.info(e);
    }

    return c;
  }

  /** {@inheritDoc} */
  public double determinant() {
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("matrix determinant");

    String check = Constants.RESULT + System.currentTimeMillis();
    Matrix c = new Matrix(config, new Text(check));
    for (int i = 0; i < getRowDimension(); i++) {
      c.set(i, 0, 1.0);
    }
    c.setDimension(getRowDimension(), 0);
    jobConf.setInputFormat(MatrixInputFormat.class);
    jobConf.setOutputFormat(MatrixOutputFormat.class);
    DeterminantMap.initJob(getName(), check, DeterminantMap.class, jobConf);
    DeterminantReduce.initJob(getName(), DeterminantReduce.class, jobConf);
    
    jobConf.setNumMapTasks(mapper);
    jobConf.setNumReduceTasks(reducer);
    
    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      LOG.info(e);
    }

    c.clear();
    return getDeterminant();
  }

  /** {@inheritDoc} */
  public TriangularMatrix decompose(Decomposition technique) {
    if (technique.equals(Decomposition.Cholesky))
      return choleskyDecompose();
    else if (technique.equals(Decomposition.Crout))
      return croutDecompose();
    else
      return null;
  }

  private TriangularMatrix croutDecompose() {
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("Crout Decomposition");

    String output = Constants.RESULT + System.currentTimeMillis();
    TriangularMatrix b = new TriangularMatrix(config, new Text(output));
    jobConf.setInputFormat(MatrixInputFormat.class);
    jobConf.setOutputFormat(MatrixOutputFormat.class);
    CroutDecompositionMap.initJob(getName(), output,
        CroutDecompositionMap.class, jobConf);

    jobConf.setNumMapTasks(mapper);
    jobConf.setNumReduceTasks(reducer);
    
    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      LOG.info(e);
    }

    return b;
  }

  private TriangularMatrix choleskyDecompose() {
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("Cholesky Decomposition");

    String output = Constants.RESULT + System.currentTimeMillis();
    TriangularMatrix b = new TriangularMatrix(config, new Text(output));
    jobConf.setInputFormat(MatrixInputFormat.class);
    jobConf.setOutputFormat(MatrixOutputFormat.class);
    CholeskyDecompositionMap.initJob(getName(), output,
        CholeskyDecompositionMap.class, jobConf);

    jobConf.setNumMapTasks(mapper);
    jobConf.setNumReduceTasks(reducer);
    
    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      LOG.info(e);
    }

    return b;
  }

}
