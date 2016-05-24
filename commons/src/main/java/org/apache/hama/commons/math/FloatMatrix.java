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

public interface FloatMatrix {

  /**
   * Not flagged value for sparse matrices, it is default to 0.0f.
   */
  public static final float NOT_FLAGGED = 0.0f;

  /**
   * Get a specific value of the matrix.
   * 
   * @return Returns the integer value at in the column at the row.
   */
  public float get(int row, int col);

  /**
   * Returns the number of columns in the matrix. Always a constant time
   * operation.
   */
  public int getColumnCount();

  /**
   * Get a whole column of the matrix as vector.
   */
  public FloatVector getColumnVector(int col);

  /**
   * Returns the number of rows in this matrix. Always a constant time
   * operation.
   */
  public int getRowCount();

  /**
   * Get a single row of the matrix as a vector.
   */
  public FloatVector getRowVector(int row);

  /**
   * Sets the value at the given row and column index.
   */
  public void set(int row, int col, float value);

  /**
   * Sets a whole column at index col with the given vector.
   */
  public void setColumnVector(int col, FloatVector column);

  /**
   * Sets the whole row at index rowIndex with the given vector.
   */
  public void setRowVector(int rowIndex, FloatVector row);

  /**
   * Multiplies this matrix (each element) with the given scalar and returns a
   * new matrix.
   */
  public FloatMatrix multiply(float scalar);

  /**
   * Multiplies this matrix with the given other matrix.
   * 
   * @param other the other matrix.
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix multiplyUnsafe(FloatMatrix other);

  /**
   * Validates the input and multiplies this matrix with the given other matrix.
   * 
   * @param other the other matrix.
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix multiply(FloatMatrix other);

  /**
   * Multiplies this matrix per element with a given matrix.
   */
  public FloatMatrix multiplyElementWiseUnsafe(FloatMatrix other);

  /**
   * Validates the input and multiplies this matrix per element with a given
   * matrix.
   * 
   * @param other the other matrix
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix multiplyElementWise(FloatMatrix other);

  /**
   * Multiplies this matrix with a given vector v. The returning vector contains
   * the sum of the rows.
   */
  public FloatVector multiplyVectorUnsafe(FloatVector v);

  /**
   * Multiplies this matrix with a given vector v. The returning vector contains
   * the sum of the rows.
   * 
   * @param v the vector
   * @return a new vector with the result of the operation.
   */
  public FloatVector multiplyVector(FloatVector v);

  /**
   * Transposes this matrix.
   */
  public FloatMatrix transpose();

  /**
   * Substracts the given amount by each element in this matrix. <br/>
   * = (amount - matrix value)
   */
  public FloatMatrix subtractBy(float amount);

  /**
   * Subtracts each element in this matrix by the given amount.<br/>
   * = (matrix value - amount)
   */
  public FloatMatrix subtract(float amount);

  /**
   * Subtracts this matrix by the given other matrix.
   */
  public FloatMatrix subtractUnsafe(FloatMatrix other);

  /**
   * Validates the input and subtracts this matrix by the given other matrix.
   * 
   * @param other
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix subtract(FloatMatrix other);

  /**
   * Subtracts each element in a column by the related element in the given
   * vector.
   */
  public FloatMatrix subtractUnsafe(FloatVector vec);

  /**
   * Validates and subtracts each element in a column by the related element in
   * the given vector.
   * 
   * @param vec
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix subtract(FloatVector vec);

  /**
   * Divides each element in a column by the related element in the given
   * vector.
   */
  public FloatMatrix divideUnsafe(FloatVector vec);

  /**
   * Validates and divides each element in a column by the related element in
   * the given vector.
   * 
   * @param vec
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix divide(FloatVector vec);

  /**
   * Divides this matrix by the given other matrix. (Per element division).
   */
  public FloatMatrix divideUnsafe(FloatMatrix other);

  /**
   * Validates and divides this matrix by the given other matrix. (Per element
   * division).
   * 
   * @param other
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix divide(FloatMatrix other);

  /**
   * Divides each element in this matrix by the given scalar.
   */
  public FloatMatrix divide(float scalar);

  /**
   * Adds the elements in the given matrix to the elements in this matrix.
   */
  public FloatMatrix add(FloatMatrix other);

  /**
   * Pows each element by the given argument. <br/>
   * = (matrix element^x)
   */
  public FloatMatrix pow(int x);

  /**
   * Returns the maximum value of the given column.
   */
  public float max(int column);

  /**
   * Returns the minimum value of the given column.
   */
  public float min(int column);

  /**
   * Sums all elements.
   */
  public float sum();

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
  public FloatMatrix slice(int rows, int cols);

  /**
   * Slices the given matrix from rowOffset-rowMax and from colOffset-colMax.
   */
  public FloatMatrix slice(int rowOffset, int rowMax, int colOffset, int colMax);

  /**
   * Apply a float function f(x) onto each element of the matrix. After
   * applying, each element of the current matrix will be changed from x to
   * f(x).
   * 
   * @param fun The function.
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix applyToElements(FloatFunction fun);

  /**
   * Apply a float float function f(x, y) onto each pair of the current matrix
   * elements and given matrix. After applying, each element of the current
   * matrix will be changed from x to f(x, y).
   * 
   * @param other The matrix contributing the second argument of the function.
   * @param fun The function that takes two arguments.
   * @return The matrix itself, supply for chain operation.
   */
  public FloatMatrix applyToElements(FloatMatrix other,
      FloatFloatFunction fun);

}
