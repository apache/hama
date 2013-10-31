/**
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
package org.apache.hama.commons.math;

/**
 * Standard matrix interface for double elements. Every implementation should
 * return a fresh new Matrix when operating with other elements.
 */
public interface DoubleMatrix {

  /**
   * Not flagged value for sparse matrices, it is default to 0.0d.
   */
  public static final double NOT_FLAGGED = 0.0d;

  /**
   * Get a specific value of the matrix.
   * 
   * @return Returns the integer value at in the column at the row.
   */
  public double get(int row, int col);

  /**
   * Returns the number of columns in the matrix. Always a constant time
   * operation.
   */
  public int getColumnCount();

  /**
   * Get a whole column of the matrix as vector.
   */
  public DoubleVector getColumnVector(int col);

  /**
   * Returns the number of rows in this matrix. Always a constant time
   * operation.
   */
  public int getRowCount();

  /**
   * Get a single row of the matrix as a vector.
   */
  public DoubleVector getRowVector(int row);

  /**
   * Sets the value at the given row and column index.
   */
  public void set(int row, int col, double value);

  /**
   * Sets a whole column at index col with the given vector.
   */
  public void setColumnVector(int col, DoubleVector column);

  /**
   * Sets the whole row at index rowIndex with the given vector.
   */
  public void setRowVector(int rowIndex, DoubleVector row);

  /**
   * Multiplies this matrix (each element) with the given scalar and returns a
   * new matrix.
   */
  public DoubleMatrix multiply(double scalar);

  /**
   * Multiplies this matrix with the given other matrix.
   * 
   * @param other the other matrix.
   * @return
   */
  public DoubleMatrix multiplyUnsafe(DoubleMatrix other);

  /**
   * Validates the input and multiplies this matrix with the given other matrix.
   * 
   * @param other the other matrix.
   * @return
   */
  public DoubleMatrix multiply(DoubleMatrix other);

  /**
   * Multiplies this matrix per element with a given matrix.
   */
  public DoubleMatrix multiplyElementWiseUnsafe(DoubleMatrix other);

  /**
   * Validates the input and multiplies this matrix per element with a given
   * matrix.
   * 
   * @param other the other matrix
   * @return
   */
  public DoubleMatrix multiplyElementWise(DoubleMatrix other);

  /**
   * Multiplies this matrix with a given vector v. The returning vector contains
   * the sum of the rows.
   */
  public DoubleVector multiplyVectorUnsafe(DoubleVector v);

  /**
   * Multiplies this matrix with a given vector v. The returning vector contains
   * the sum of the rows.
   * 
   * @param v the vector
   * @return
   */
  public DoubleVector multiplyVector(DoubleVector v);

  /**
   * Transposes this matrix.
   */
  public DoubleMatrix transpose();

  /**
   * Substracts the given amount by each element in this matrix. <br/>
   * = (amount - matrix value)
   */
  public DoubleMatrix subtractBy(double amount);

  /**
   * Subtracts each element in this matrix by the given amount.<br/>
   * = (matrix value - amount)
   */
  public DoubleMatrix subtract(double amount);

  /**
   * Subtracts this matrix by the given other matrix.
   */
  public DoubleMatrix subtractUnsafe(DoubleMatrix other);

  /**
   * Validates the input and subtracts this matrix by the given other matrix.
   * 
   * @param other
   * @return
   */
  public DoubleMatrix subtract(DoubleMatrix other);

  /**
   * Subtracts each element in a column by the related element in the given
   * vector.
   */
  public DoubleMatrix subtractUnsafe(DoubleVector vec);

  /**
   * Validates and subtracts each element in a column by the related element in
   * the given vector.
   * 
   * @param vec
   * @return
   */
  public DoubleMatrix subtract(DoubleVector vec);

  /**
   * Divides each element in a column by the related element in the given
   * vector.
   */
  public DoubleMatrix divideUnsafe(DoubleVector vec);

  /**
   * Validates and divides each element in a column by the related element in
   * the given vector.
   * 
   * @param vec
   * @return
   */
  public DoubleMatrix divide(DoubleVector vec);

  /**
   * Divides this matrix by the given other matrix. (Per element division).
   */
  public DoubleMatrix divideUnsafe(DoubleMatrix other);

  /**
   * Validates and divides this matrix by the given other matrix. (Per element
   * division).
   * 
   * @param other
   * @return
   */
  public DoubleMatrix divide(DoubleMatrix other);

  /**
   * Divides each element in this matrix by the given scalar.
   */
  public DoubleMatrix divide(double scalar);

  /**
   * Adds the elements in the given matrix to the elements in this matrix.
   */
  public DoubleMatrix add(DoubleMatrix other);

  /**
   * Pows each element by the given argument. <br/>
   * = (matrix element^x)
   */
  public DoubleMatrix pow(int x);

  /**
   * Returns the maximum value of the given column.
   */
  public double max(int column);

  /**
   * Returns the minimum value of the given column.
   */
  public double min(int column);

  /**
   * Sums all elements.
   */
  public double sum();

  /**
   * Returns an array of column indices existing in this matrix.
   */
  public int[] columnIndices();

  /**
   * Returns true if the underlying implementation is sparse.
   */
  public boolean isSparse();

  /**
   * Slices the given matrix from 0-rows and from 0-columns.
   */
  public DoubleMatrix slice(int rows, int cols);

  /**
   * Slices the given matrix from rowOffset-rowMax and from colOffset-colMax.
   */
  public DoubleMatrix slice(int rowOffset, int rowMax, int colOffset, int colMax);

  /**
   * Apply a double function f(x) onto each element of the matrix. After
   * applying, each element of the current matrix will be changed from x to
   * f(x).
   * 
   * @param fun The function.
   * @return The matrix itself, supply for chain operation.
   */
  public DoubleMatrix applyToElements(DoubleFunction fun);

  /**
   * Apply a double double function f(x, y) onto each pair of the current matrix
   * elements and given matrix. After applying, each element of the current
   * matrix will be changed from x to f(x, y).
   * 
   * @param other The matrix contributing the second argument of the function.
   * @param fun The function that takes two arguments.
   * @return The matrix itself, supply for chain operation.
   */
  public DoubleMatrix applyToElements(DoubleMatrix other,
      DoubleDoubleFunction fun);

}
