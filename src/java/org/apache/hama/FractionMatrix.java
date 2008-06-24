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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.algebra.CroutDecompositionMap;
import org.apache.hama.mapred.FractionMatrixInputFormat;
import org.apache.hama.mapred.MatrixOutputFormat;

/**
 * A library for mathematical operations on matrices of fractions.
 */
public class FractionMatrix extends AbstractMatrix {

  /**
   * Construct
   * 
   * @param conf configuration object
   */
  public FractionMatrix(Configuration conf) {
    setConfiguration(conf);
  }

  /**
   * Construct an fraction matrix
   * 
   * @param conf configuration object
   * @param matrixName the name of the matrix
   */
  public FractionMatrix(Configuration conf, Text matrixName) {
    try {
      setConfiguration(conf);
      this.matrixName = matrixName;

      if (!admin.tableExists(matrixName)) {
        tableDesc = new HTableDescriptor(matrixName.toString());
        tableDesc.addFamily(new HColumnDescriptor(Constants.NUMERATOR.toString()));
        tableDesc.addFamily(new HColumnDescriptor(Constants.DENOMINATOR.toString()));
        tableDesc.addFamily(new HColumnDescriptor(Constants.ORIGINAL.toString()));
        create();
      }

      table = new HTable(config, matrixName);
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
    if(technique.equals(Decomposition.Crout)) 
       return croutDecompose();
    else return null;
  }

  private TriangularMatrix croutDecompose() {
    JobConf jobConf = new JobConf(config);
    jobConf.setJobName("Crout Decomposition");

    String output = Constants.RESULT + System.currentTimeMillis();
    TriangularMatrix b = new TriangularMatrix(config, new Text(output));

    jobConf.setInputFormat(FractionMatrixInputFormat.class);
    jobConf.setOutputFormat(MatrixOutputFormat.class);
    CroutDecompositionMap.initJob(getName(), output, CroutDecompositionMap.class,
        jobConf);

    jobConf.setNumMapTasks(mapper);
    jobConf.setNumReduceTasks(reducer);
    
    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      LOG.info(e);
    }

    return b;
  }

  /**
   * Gets the entry value of numerator matrix N(i, j)
   * 
   * @param i ith row of numerator matrix N
   * @param j jth column of numerator matrix N
   * @return the value of entry
   */
  public double getFromNumerator(int i, int j) {
    try {
      return toDouble(table.get(new Text(String.valueOf(i)), new Text(
          Constants.NUMERATOR + String.valueOf(j))).getValue());
    } catch (IOException e) {
      LOG.error(e, e);
      return 0;
    }
  }

  /**
   * Sets the double value of numerator matrix N(i, j)
   * 
   * @param i ith row of numerator matrix N
   * @param j jth column of numerator matrix N
   * @param d the value of entry
   */
  public void setToNumerator(int i, int j, double d) {
    BatchUpdate b = new BatchUpdate(new Text(String.valueOf(i)));
    b.put(new Text(Constants.NUMERATOR + String.valueOf(j)), toBytes(d));
    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /**
   * Gets the entry value of denominator matrix D(i, j)
   * 
   * @param i ith row of denominator matrix D
   * @param j jth column of denominator matrix D
   * @return the value of entry
   */
  public double getFromDenominator(int i, int j) {
    try {
      return toDouble(table.get(new Text(String.valueOf(i)), new Text(
          Constants.DENOMINATOR + String.valueOf(j))).getValue());
    } catch (IOException e) {
      LOG.error(e, e);
      return 0;
    }
  }

  /**
   * Sets the double value of denominator matrix D(i, j)
   * 
   * @param i ith row of denominator matrix D
   * @param j jth column of denominator matrix D
   * @param d the value of entry
   */
  public void setToDenominator(int i, int j, double d) {
    BatchUpdate b = new BatchUpdate(new Text(String.valueOf(i)));
    b.put(new Text(Constants.DENOMINATOR + String.valueOf(j)), toBytes(d));
    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /**
   * Sets the double value of original matrix O(i, j)
   * 
   * @param i ith row of original matrix O
   * @param j jth column of original matrix O
   * @param d the value of entry
   */
  public void setToOriginal(int i, int j, double d) {
    BatchUpdate b = new BatchUpdate(new Text(String.valueOf(i)));
    b.put(new Text(Constants.ORIGINAL + String.valueOf(j)), toBytes(d));
    try {
      table.commit(b);
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }

  /**
   * Gets the entry value of original matrix O(i, j)
   * 
   * @param i ith row of original matrix O
   * @param j jth column of original matrix O
   * @return a double
   */
  public double getFromOriginal(int i, int j) {
    try {
      return toDouble(table.get(new Text(String.valueOf(i)),
          new Text(Constants.ORIGINAL + String.valueOf(j))).getValue());
    } catch (IOException e) {
      LOG.error(e, e);
      return 0;
    }
  }

}
