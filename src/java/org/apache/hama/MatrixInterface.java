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


/**
 * Basic matrix interface. It holds <code>double</code>s in a rectangular 2D
 * array, and it is used alongside <code>Vector</code> in numerical
 * computations. Implementing classes decides on the actual storage.
 */
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
  public FeatureVector getRowVector(int row);

  /**
   * Sets the double value of (i, j)
   * 
   * @param i ith row of the matrix
   * @param j jth column of the matrix
   * @param d the value of entry
   */
  public void set(int i, int j, double d);

  /**
   * Adds value to (i, j)
   * 
   * @param i i th row of the matrix
   * @param j j th column of the matrix
   * @param d the value of entry
   */
  public void add(int i, int j, double d);

  /**
   * Delete a Row to Matrix.
   * 
   * @param i row number to delete
   */
  public void deleteRowEquals(int i);

  /**
   * Delete a Column to Matrix.
   * 
   * @param j column number to delete
   */
  public void deleteColumnEquals(int j);

  /**
   * Get a number of row of the matrix from the meta-data column
   * 
   * @return a number of rows of the matrix
   */
  public int getRowDimension();

  /**
   * Get a number of column of the matrix from the meta-data column
   * 
   * @return a number of columns of the matrix
   */
  public int getColumnDimension();

  /**
   * Sets the dimension of matrix
   * 
   * @param rows the number of rows
   * @param columns the number of columns
   */
  public void setDimension(int rows, int columns);

  /**
   * Modify dimensions of matrix
   * 
   * @param m number of rows
   * @param n number of columns
   */
  public void reset(int m, int n);

  /**
   * Return the matrix name
   * 
   * @return the name of the matrix
   */
  public String getName();

  /**
   * Make a deep copy of a matrix
   * 
   * @return clone matrix
   */
  public Matrix copy();

  /**
   * Multiply two matrices
   * 
   * @param b matrix b
   * @return the result of the multiplication of matrix a and matrix b
   */
  public Matrix multiply(Matrix b);

  /**
   * Add two matrices
   * 
   * @param b matrix b
   * @return the result of the addition of matrix a and matrix b
   */
  public Matrix addition(Matrix b);

  /**
   * Subtract two matrices
   * 
   * @param b matrix b
   * @return the result of the substraction of matrix a and matrix b
   */
  public Matrix subtraction(Matrix b);

  /**
   * Calculates determinant of a matrix
   * 
   * @return the value of determinant
   */
  public double determinant();

  /**
   * Save the matrix to table
   * 
   * @param matrixName the name of the matrix
   */
  public void save(String matrixName);

  /**
   * Decomposition
   * 
   * @return the decomposed result
   */
  public TriangularMatrix decompose(Decomposition technique);

  /**
   * Clear object
   */
  public void clear();

  /**
   * Close object
   */
  public void close();

}
