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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import com.google.common.base.Preconditions;

public final class DenseFloatMatrix implements FloatMatrix {

  protected float[][] matrix;
  protected int numRows;
  protected int numColumns;

  public DenseFloatMatrix() { }
  
  /**
   * Creates a new empty matrix from the rows and columns.
   * 
   * @param rows the num of rows.
   * @param columns the num of columns.
   */
  public DenseFloatMatrix(int rows, int columns) {
    this.numRows = rows;
    this.numColumns = columns;
    this.matrix = new float[rows][columns];
  }

  /**
   * Creates a new empty matrix from the rows and columns filled with the given
   * default value.
   * 
   * @param rows the num of rows.
   * @param columns the num of columns.
   * @param defaultValue the default value.
   */
  public DenseFloatMatrix(int rows, int columns, float defaultValue) {
    this.numRows = rows;
    this.numColumns = columns;
    this.matrix = new float[rows][columns];

    for (int i = 0; i < numRows; i++) {
      Arrays.fill(matrix[i], defaultValue);
    }
  }

  /**
   * Creates a new empty matrix from the rows and columns filled with the given
   * random values.
   * 
   * @param rows the num of rows.
   * @param columns the num of columns.
   * @param rand the random instance to use.
   */
  public DenseFloatMatrix(int rows, int columns, Random rand) {
    this.numRows = rows;
    this.numColumns = columns;
    this.matrix = new float[rows][columns];

    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        matrix[i][j] = rand.nextFloat();
      }
    }
  }

  /**
   * Simple copy constructor, but does only bend the reference to this instance.
   * 
   * @param otherMatrix the other matrix.
   */
  public DenseFloatMatrix(float[][] otherMatrix) {
    this.matrix = otherMatrix;
    this.numRows = otherMatrix.length;
    if (matrix.length > 0)
      this.numColumns = matrix[0].length;
    else
      this.numColumns = numRows;
  }

  /**
   * Generates a matrix out of an vector array. it treats the array entries as
   * rows and the vector itself contains the values of the columns.
   * 
   * @param vectorArray the array of vectors.
   */
  public DenseFloatMatrix(FloatVector[] vectorArray) {
    this.matrix = new float[vectorArray.length][];
    this.numRows = vectorArray.length;

    for (int i = 0; i < vectorArray.length; i++) {
      this.setRowVector(i, vectorArray[i]);
    }

    if (matrix.length > 0)
      this.numColumns = matrix[0].length;
    else
      this.numColumns = numRows;
  }

  /**
   * Sets the first column of this matrix to the given vector.
   * 
   * @param first the new first column of the given vector
   */
  public DenseFloatMatrix(DenseFloatVector first) {
    this(first.getLength(), 1);
    setColumn(0, first.toArray());
  }

  /**
   * Copies the given float array v into the first row of this matrix, and
   * creates this with the number of given rows and columns.
   * 
   * @param v the values to put into the first row.
   * @param rows the number of rows.
   * @param columns the number of columns.
   */
  public DenseFloatMatrix(float[] v, int rows, int columns) {
    this.matrix = new float[rows][columns];

    for (int i = 0; i < rows; i++) {
      System.arraycopy(v, i * columns, this.matrix[i], 0, columns);
    }

    int index = 0;
    for (int col = 0; col < columns; col++) {
      for (int row = 0; row < rows; row++) {
        matrix[row][col] = v[index++];
      }
    }

    this.numRows = rows;
    this.numColumns = columns;
  }

  /**
   * Creates a new matrix with the given vector into the first column and the
   * other matrix to the other columns.
   * 
   * @param first the new first column.
   * @param otherMatrix the other matrix to set on from the second column.
   */
  public DenseFloatMatrix(DenseFloatVector first, FloatMatrix otherMatrix) {
    this(otherMatrix.getRowCount(), otherMatrix.getColumnCount() + 1);
    setColumn(0, first.toArray());
    for (int col = 1; col < otherMatrix.getColumnCount() + 1; col++)
      setColumnVector(col, otherMatrix.getColumnVector(col - 1));
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#get(int, int)
   */
  @Override
  public final float get(int row, int col) {
    return this.matrix[row][col];
  }

  /**
   * Gets a whole column of the matrix as a float array.
   */
  public final float[] getColumn(int col) {
    final float[] column = new float[numRows];
    for (int r = 0; r < numRows; r++) {
      column[r] = matrix[r][col];
    }
    return column;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#getColumnCount()
   */
  @Override
  public final int getColumnCount() {
    return numColumns;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#getColumnVector(int)
   */
  @Override
  public final FloatVector getColumnVector(int col) {
    return new DenseFloatVector(getColumn(col));
  }

  /**
   * Get the matrix as 2-dimensional float array (first dimension is the row,
   * second the column) to faster access the values.
   */
  public final float[][] getValues() {
    return matrix;
  }

  /**
   * Get a single row of the matrix as a float array.
   */
  public final float[] getRow(int row) {
    return matrix[row];
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#getRowCount()
   */
  @Override
  public final int getRowCount() {
    return numRows;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#getRowVector(int)
   */
  @Override
  public final FloatVector getRowVector(int row) {
    return new DenseFloatVector(getRow(row));
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#set(int, int, float)
   */
  @Override
  public final void set(int row, int col, float value) {
    this.matrix[row][col] = value;
  }

  /**
   * Sets the row to a given float array. This does not copy, rather than just
   * bends the references.
   */
  public final void setRow(int row, float[] value) {
    this.matrix[row] = value;
  }

  /**
   * Sets the column to a given float array. This does not copy, rather than
   * just bends the references.
   */
  public final void setColumn(int col, float[] values) {
    for (int i = 0; i < getRowCount(); i++) {
      this.matrix[i][col] = values[i];
    }
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#setColumnVector(int,
   * de.jungblut.math.FloatVector)
   */
  @Override
  public void setColumnVector(int col, FloatVector column) {
    this.setColumn(col, column.toArray());
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#setRowVector(int,
   * de.jungblut.math.FloatVector)
   */
  @Override
  public void setRowVector(int rowIndex, FloatVector row) {
    this.setRow(rowIndex, row.toArray());
  }

  /**
   * Returns the size of the matrix as string (ROWSxCOLUMNS).
   */
  public String sizeToString() {
    return numRows + "x" + numColumns;
  }

  /**
   * Splits the last column from this matrix. Usually used to get a prediction
   * column from some machine learning problem.
   * 
   * @return a tuple of a new sliced matrix and a vector which was the last
   *         column.
   */
  public final Tuple<DenseFloatMatrix, DenseFloatVector> splitLastColumn() {
    DenseFloatMatrix m = new DenseFloatMatrix(getRowCount(),
        getColumnCount() - 1);
    for (int i = 0; i < getRowCount(); i++) {
      for (int j = 0; j < getColumnCount() - 1; j++) {
        m.set(i, j, get(i, j));
      }
    }
    DenseFloatVector v = new DenseFloatVector(getColumn(getColumnCount() - 1));
    return new Tuple<DenseFloatMatrix, DenseFloatVector>(m, v);
  }

  /**
   * Creates two matrices out of this by the given percentage. It uses a random
   * function to determine which rows should belong to the matrix including the
   * given percentage amount of rows.
   * 
   * @param percentage A float value between 0.0f and 1.0f
   * @return A tuple which includes two matrices, the first contains the
   *         percentage of the rows from the original matrix (rows are chosen
   *         randomly) and the second one contains all other rows.
   */
  public final Tuple<DenseFloatMatrix, DenseFloatMatrix> splitRandomMatrices(
      float percentage) {
    if (percentage < 0.0f || percentage > 1.0f) {
      throw new IllegalArgumentException(
          "Percentage must be between 0.0 and 1.0! Given " + percentage);
    }

    if (percentage == 1.0f) {
      return new Tuple<DenseFloatMatrix, DenseFloatMatrix>(this, null);
    } else if (percentage == 0.0f) {
      return new Tuple<DenseFloatMatrix, DenseFloatMatrix>(null, this);
    }

    final Random rand = new Random(System.nanoTime());
    int firstMatrixRowsCount = Math.round(percentage * numRows);

    // we first choose needed rows number of items to pick
    final HashSet<Integer> lowerMatrixRowIndices = new HashSet<Integer>();
    int missingRows = firstMatrixRowsCount;
    while (missingRows > 0) {
      final int nextIndex = rand.nextInt(numRows);
      if (lowerMatrixRowIndices.add(nextIndex)) {
        missingRows--;
      }
    }

    // make to new matrixes
    final float[][] firstMatrix = new float[firstMatrixRowsCount][numColumns];
    int firstMatrixIndex = 0;
    final float[][] secondMatrix = new float[numRows - firstMatrixRowsCount][numColumns];
    int secondMatrixIndex = 0;

    // then we loop over all items and put split the matrix
    for (int r = 0; r < numRows; r++) {
      if (lowerMatrixRowIndices.contains(r)) {
        firstMatrix[firstMatrixIndex++] = matrix[r];
      } else {
        secondMatrix[secondMatrixIndex++] = matrix[r];
      }
    }

    return new Tuple<DenseFloatMatrix, DenseFloatMatrix>(
        new DenseFloatMatrix(firstMatrix), new DenseFloatMatrix(secondMatrix));
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#multiply(float)
   */
  @Override
  public final DenseFloatMatrix multiply(float scalar) {
    DenseFloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] * scalar);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#multiply(de.jungblut.math.FloatMatrix)
   */
  @Override
  public final FloatMatrix multiplyUnsafe(FloatMatrix other) {
    DenseFloatMatrix matrix = new DenseFloatMatrix(this.getRowCount(),
        other.getColumnCount());

    final int m = this.numRows;
    final int n = this.numColumns;
    final int p = other.getColumnCount();

    for (int j = p; --j >= 0;) {
      for (int i = m; --i >= 0;) {
        float s = 0;
        for (int k = n; --k >= 0;) {
          s += get(i, k) * other.get(k, j);
        }
        matrix.set(i, j, s + matrix.get(i, j));
      }
    }

    return matrix;
  }

  /*
   * (non-Javadoc)
   * @see
   * de.jungblut.math.FloatMatrix#multiplyElementWise(de.jungblut.math.FloatMatrix
   * )
   */
  @Override
  public final FloatMatrix multiplyElementWiseUnsafe(FloatMatrix other) {
    DenseFloatMatrix matrix = new DenseFloatMatrix(this.numRows,
        this.numColumns);

    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        matrix.set(i, j, this.get(i, j) * (other.get(i, j)));
      }
    }

    return matrix;
  }

  /*
   * (non-Javadoc)
   * @see
   * de.jungblut.math.FloatMatrix#multiplyVector(de.jungblut.math.FloatVector)
   */
  @Override
  public final FloatVector multiplyVectorUnsafe(FloatVector v) {
    FloatVector vector = new DenseFloatVector(this.getRowCount());
    for (int row = 0; row < numRows; row++) {
      float sum = 0.0f;
      for (int col = 0; col < numColumns; col++) {
        sum += (matrix[row][col] * v.get(col));
      }
      vector.set(row, sum);
    }

    return vector;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#transpose()
   */
  @Override
  public DenseFloatMatrix transpose() {
    DenseFloatMatrix m = new DenseFloatMatrix(this.numColumns, this.numRows);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(j, i, this.matrix[i][j]);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#subtractBy(float)
   */
  @Override
  public DenseFloatMatrix subtractBy(float amount) {
    DenseFloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, amount - this.matrix[i][j]);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#subtract(float)
   */
  @Override
  public DenseFloatMatrix subtract(float amount) {
    DenseFloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] - amount);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#subtract(de.jungblut.math.FloatMatrix)
   */
  @Override
  public FloatMatrix subtractUnsafe(FloatMatrix other) {
    FloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] - other.get(i, j));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#subtract(de.jungblut.math.FloatVector)
   */
  @Override
  public DenseFloatMatrix subtractUnsafe(FloatVector vec) {
    DenseFloatMatrix cop = new DenseFloatMatrix(this.getRowCount(),
        this.getColumnCount());
    for (int i = 0; i < this.getColumnCount(); i++) {
      cop.setColumn(i, getColumnVector(i).subtract(vec.get(i)).toArray());
    }
    return cop;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#divide(de.jungblut.math.FloatVector)
   */
  @Override
  public FloatMatrix divideUnsafe(FloatVector vec) {
    FloatMatrix cop = new DenseFloatMatrix(this.getRowCount(),
        this.getColumnCount());
    for (int i = 0; i < this.getColumnCount(); i++) {
      cop.setColumnVector(i, getColumnVector(i).divide(vec.get(i)));
    }
    return cop;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FloatMatrix divide(FloatVector vec) {
    Preconditions.checkArgument(this.getColumnCount() == vec.getDimension(),
        "Dimension mismatch.");
    return this.divideUnsafe(vec);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#divide(de.jungblut.math.FloatMatrix)
   */
  @Override
  public FloatMatrix divideUnsafe(FloatMatrix other) {
    FloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] / other.get(i, j));
      }
    }
    return m;
  }

  @Override
  public FloatMatrix divide(FloatMatrix other) {
    Preconditions.checkArgument(this.getRowCount() == other.getRowCount()
        && this.getColumnCount() == other.getColumnCount());
    return divideUnsafe(other);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#divide(float)
   */
  @Override
  public FloatMatrix divide(float scalar) {
    FloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] / scalar);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#add(de.jungblut.math.FloatMatrix)
   */
  @Override
  public FloatMatrix add(FloatMatrix other) {
    FloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] + other.get(i, j));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#pow(int)
   */
  @Override
  public FloatMatrix pow(int x) {
    FloatMatrix m = new DenseFloatMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, (float) Math.pow(matrix[i][j], x));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#max(int)
   */
  @Override
  public float max(int column) {
    float max = Float.MIN_VALUE;
    for (int i = 0; i < getRowCount(); i++) {
      float d = matrix[i][column];
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#min(int)
   */
  @Override
  public float min(int column) {
    float min = Float.MAX_VALUE;
    for (int i = 0; i < getRowCount(); i++) {
      float d = matrix[i][column];
      if (d < min) {
        min = d;
      }
    }
    return min;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#slice(int, int)
   */
  @Override
  public FloatMatrix slice(int rows, int cols) {
    return slice(0, rows, 0, cols);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#slice(int, int, int, int)
   */
  @Override
  public FloatMatrix slice(int rowOffset, int rowMax, int colOffset, int colMax) {
    DenseFloatMatrix m = new DenseFloatMatrix(rowMax - rowOffset, colMax
        - colOffset);
    for (int row = rowOffset; row < rowMax; row++) {
      for (int col = colOffset; col < colMax; col++) {
        m.set(row - rowOffset, col - colOffset, this.get(row, col));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#isSparse()
   */
  @Override
  public boolean isSparse() {
    return false;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#sum()
   */
  @Override
  public float sum() {
    float x = 0.0f;
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        x += Math.abs(matrix[i][j]);
      }
    }
    return x;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatMatrix#columnIndices()
   */
  @Override
  public int[] columnIndices() {
    int[] x = new int[getColumnCount()];
    for (int i = 0; i < getColumnCount(); i++)
      x[i] = i;
    return x;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(matrix);
    result = prime * result + numColumns;
    result = prime * result + numRows;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DenseFloatMatrix other = (DenseFloatMatrix) obj;
    if (!Arrays.deepEquals(matrix, other.matrix))
      return false;
    if (numColumns != other.numColumns)
      return false;
    return numRows == other.numRows;
  }

  @Override
  public String toString() {
    if (numRows < 10) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numRows; i++) {
        sb.append(Arrays.toString(matrix[i]));
        sb.append('\n');
      }
      return sb.toString();
    } else {
      return numRows + "x" + numColumns;
    }
  }

  /**
   * Gets the eye matrix (ones on the main diagonal) with a given dimension.
   */
  public static DenseFloatMatrix eye(int dimension) {
    DenseFloatMatrix m = new DenseFloatMatrix(dimension, dimension);

    for (int i = 0; i < dimension; i++) {
      m.set(i, i, 1);
    }

    return m;
  }

  /**
   * Deep copies the given matrix into a new returned one.
   */
  public static DenseFloatMatrix copy(DenseFloatMatrix matrix) {
    final float[][] src = matrix.getValues();
    final float[][] dest = new float[matrix.getRowCount()][matrix
        .getColumnCount()];

    for (int i = 0; i < dest.length; i++)
      System.arraycopy(src[i], 0, dest[i], 0, src[i].length);

    return new DenseFloatMatrix(dest);
  }

  /**
   * Some strange function I found in octave but I don't know what it was named.
   * It does however multiply the elements from the transposed vector and the
   * normal vector and sets it into the according indices of a new constructed
   * matrix.
   */
  public static DenseFloatMatrix multiplyTransposedVectors(
      FloatVector transposed, FloatVector normal) {
    DenseFloatMatrix m = new DenseFloatMatrix(transposed.getLength(),
        normal.getLength());
    for (int row = 0; row < transposed.getLength(); row++) {
      for (int col = 0; col < normal.getLength(); col++) {
        m.set(row, col, transposed.get(row) * normal.get(col));
      }
    }

    return m;
  }

  /**
   * Just a absolute error function.
   */
  public static float error(DenseFloatMatrix a, DenseFloatMatrix b) {
    return a.subtractUnsafe(b).sum();
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public FloatMatrix applyToElements(FloatFunction fun) {
    for (int r = 0; r < this.numRows; ++r) {
      for (int c = 0; c < this.numColumns; ++c) {
        this.set(r, c, fun.apply(this.get(r, c)));
      }
    }
    return this;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public FloatMatrix applyToElements(FloatMatrix other,
      FloatFloatFunction fun) {
    Preconditions
        .checkArgument(this.numRows == other.getRowCount()
            && this.numColumns == other.getColumnCount(),
            "Cannot apply float float function to matrices with different sizes.");

    for (int r = 0; r < this.numRows; ++r) {
      for (int c = 0; c < this.numColumns; ++c) {
        this.set(r, c, fun.apply(this.get(r, c), other.get(r, c)));
      }
    }

    return this;
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.ml.math.FloatMatrix#safeMultiply(org.apache.hama.ml.math
   * .FloatMatrix)
   */
  @Override
  public FloatMatrix multiply(FloatMatrix other) {
    Preconditions
        .checkArgument(
            this.numColumns == other.getRowCount(),
            String
                .format(
                    "Matrix with size [%d, %d] cannot multiple matrix with size [%d, %d]",
                    this.numRows, this.numColumns, other.getRowCount(),
                    other.getColumnCount()));

    return this.multiplyUnsafe(other);
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.ml.math.FloatMatrix#safeMultiplyElementWise(org.apache
   * .hama.ml.math.FloatMatrix)
   */
  @Override
  public FloatMatrix multiplyElementWise(FloatMatrix other) {
    Preconditions.checkArgument(this.numRows == other.getRowCount()
        && this.numColumns == other.getColumnCount(),
        "Matrices with different dimensions cannot be multiplied elementwise.");
    return this.multiplyElementWiseUnsafe(other);
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.ml.math.FloatMatrix#safeMultiplyVector(org.apache.hama
   * .ml.math.FloatVector)
   */
  @Override
  public FloatVector multiplyVector(FloatVector v) {
    Preconditions.checkArgument(this.numColumns == v.getDimension(),
        "Dimension mismatch.");
    return this.multiplyVectorUnsafe(v);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.ml.math.FloatMatrix#subtract(org.apache.hama.ml.math.
   * FloatMatrix)
   */
  @Override
  public FloatMatrix subtract(FloatMatrix other) {
    Preconditions.checkArgument(this.numRows == other.getRowCount()
        && this.numColumns == other.getColumnCount(), "Dimension mismatch.");
    return subtractUnsafe(other);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.ml.math.FloatMatrix#subtract(org.apache.hama.ml.math.
   * FloatVector)
   */
  @Override
  public FloatMatrix subtract(FloatVector vec) {
    Preconditions.checkArgument(this.numColumns == vec.getDimension(),
        "Dimension mismatch.");
    return null;
  }

}
