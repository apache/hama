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

@Deprecated
public interface MatrixInterface {

  /**
   * Gets the double value of (i, j)
   * 
   * @param i ith row of the matrix
   * @param j jth column of the matrix
   * @return the value of entry
   */
  public double get(int i, int j);

  /**
   * Gets the vector of row
   * 
   * @param row the row index of the matrix
   * @return the feature vector of row
   */
  public Vector getRow(int row);

  /**
   * Get a number of row of the matrix from the meta-data column
   * 
   * @return a number of rows of the matrix
   */
  public int getRows();

  /**
   * Get a number of column of the matrix from the meta-data column
   * 
   * @return a number of columns of the matrix
   */
  public int getColumns();

  /**
   * Sets the double value of (i, j)
   * 
   * @param i ith row of the matrix
   * @param j jth column of the matrix
   * @param value the value of entry
   */
  public void set(int i, int j, double value);

  /**
   * A=alpha*B
   * 
   * @param alpha
   * @param B
   * @return A
   */
  public Matrix set(double alpha, Matrix B);

  /**
   * A=B
   * 
   * @param B
   * @return A
   */
  public Matrix set(Matrix B);

  /**
   * Sets the dimension of matrix
   * 
   * @param rows the number of rows
   * @param columns the number of columns
   */
  public void setDimension(int rows, int columns);

  /**
   * A(i, j) += value
   * 
   * @param i
   * @param j
   * @param value
   */
  public void add(int i, int j, double value);

  /**
   * A = B + A
   * 
   * @param B
   * @return A
   */
  public Matrix add(Matrix B);

  /**
   * A = alpha*B + A
   * 
   * @param alpha
   * @param B
   * @return A
   */
  public Matrix add(double alpha, Matrix B);

  /**
   * C = A*B
   * 
   * @param B
   * @return C
   */
  public Matrix mult(Matrix B);

  /**
   * C = alpha*A*B + C
   * 
   * @param alpha
   * @param B
   * @param C
   * @return C
   */
  public Matrix multAdd(double alpha, Matrix B, Matrix C);

  /**
   * Computes the given norm of the matrix
   * 
   * @param type
   * @return norm of the matrix
   */
  public double norm(Norm type);

  /**
   * Supported matrix-norms.
   */
  enum Norm {
    // TODO
  }

  /**
   * Return the matrix name
   * 
   * @return the name of the matrix
   */
  public String getName();
}
