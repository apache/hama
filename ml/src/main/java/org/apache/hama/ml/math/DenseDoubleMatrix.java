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
package org.apache.hama.ml.math;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

/**
 * Dense double matrix implementation, internally uses two dimensional double
 * arrays.
 */
public final class DenseDoubleMatrix implements DoubleMatrix {

  protected final double[][] matrix;
  protected final int numRows;
  protected final int numColumns;

  /**
   * Creates a new empty matrix from the rows and columns.
   * 
   * @param rows the num of rows.
   * @param columns the num of columns.
   */
  public DenseDoubleMatrix(int rows, int columns) {
    this.numRows = rows;
    this.numColumns = columns;
    this.matrix = new double[rows][columns];
  }

  /**
   * Creates a new empty matrix from the rows and columns filled with the given
   * default value.
   * 
   * @param rows the num of rows.
   * @param columns the num of columns.
   * @param defaultValue the default value.
   */
  public DenseDoubleMatrix(int rows, int columns, double defaultValue) {
    this.numRows = rows;
    this.numColumns = columns;
    this.matrix = new double[rows][columns];

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
  public DenseDoubleMatrix(int rows, int columns, Random rand) {
    this.numRows = rows;
    this.numColumns = columns;
    this.matrix = new double[rows][columns];

    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        matrix[i][j] = rand.nextDouble();
      }
    }
  }

  /**
   * Simple copy constructor, but does only bend the reference to this instance.
   * 
   * @param otherMatrix the other matrix.
   */
  public DenseDoubleMatrix(double[][] otherMatrix) {
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
  public DenseDoubleMatrix(DoubleVector[] vectorArray) {
    this.matrix = new double[vectorArray.length][];
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
  public DenseDoubleMatrix(DenseDoubleVector first) {
    this(first.getLength(), 1);
    setColumn(0, first.toArray());
  }

  /**
   * Copies the given double array v into the first row of this matrix, and
   * creates this with the number of given rows and columns.
   * 
   * @param v the values to put into the first row.
   * @param rows the number of rows.
   * @param columns the number of columns.
   */
  public DenseDoubleMatrix(double[] v, int rows, int columns) {
    this.matrix = new double[rows][columns];

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
  public DenseDoubleMatrix(DenseDoubleVector first, DoubleMatrix otherMatrix) {
    this(otherMatrix.getRowCount(), otherMatrix.getColumnCount() + 1);
    setColumn(0, first.toArray());
    for (int col = 1; col < otherMatrix.getColumnCount() + 1; col++)
      setColumnVector(col, otherMatrix.getColumnVector(col - 1));
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#get(int, int)
   */
  @Override
  public final double get(int row, int col) {
    return this.matrix[row][col];
  }

  /**
   * Gets a whole column of the matrix as a double array.
   */
  public final double[] getColumn(int col) {
    final double[] column = new double[numRows];
    for (int r = 0; r < numRows; r++) {
      column[r] = matrix[r][col];
    }
    return column;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#getColumnCount()
   */
  @Override
  public final int getColumnCount() {
    return numColumns;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#getColumnVector(int)
   */
  @Override
  public final DoubleVector getColumnVector(int col) {
    return new DenseDoubleVector(getColumn(col));
  }

  /**
   * Get the matrix as 2-dimensional double array (first dimension is the row,
   * second the column) to faster access the values.
   */
  public final double[][] getValues() {
    return matrix;
  }

  /**
   * Get a single row of the matrix as a double array.
   */
  public final double[] getRow(int row) {
    return matrix[row];
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#getRowCount()
   */
  @Override
  public final int getRowCount() {
    return numRows;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#getRowVector(int)
   */
  @Override
  public final DoubleVector getRowVector(int row) {
    return new DenseDoubleVector(getRow(row));
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#set(int, int, double)
   */
  @Override
  public final void set(int row, int col, double value) {
    this.matrix[row][col] = value;
  }

  /**
   * Sets the row to a given double array. This does not copy, rather than just
   * bends the references.
   */
  public final void setRow(int row, double[] value) {
    this.matrix[row] = value;
  }

  /**
   * Sets the column to a given double array. This does not copy, rather than
   * just bends the references.
   */
  public final void setColumn(int col, double[] values) {
    for (int i = 0; i < getRowCount(); i++) {
      this.matrix[i][col] = values[i];
    }
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#setColumnVector(int,
   * de.jungblut.math.DoubleVector)
   */
  @Override
  public void setColumnVector(int col, DoubleVector column) {
    this.setColumn(col, column.toArray());
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#setRowVector(int,
   * de.jungblut.math.DoubleVector)
   */
  @Override
  public void setRowVector(int rowIndex, DoubleVector row) {
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
  public final Tuple<DenseDoubleMatrix, DenseDoubleVector> splitLastColumn() {
    DenseDoubleMatrix m = new DenseDoubleMatrix(getRowCount(),
        getColumnCount() - 1);
    for (int i = 0; i < getRowCount(); i++) {
      for (int j = 0; j < getColumnCount() - 1; j++) {
        m.set(i, j, get(i, j));
      }
    }
    DenseDoubleVector v = new DenseDoubleVector(getColumn(getColumnCount() - 1));
    return new Tuple<DenseDoubleMatrix, DenseDoubleVector>(m, v);
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
  public final Tuple<DenseDoubleMatrix, DenseDoubleMatrix> splitRandomMatrices(
      float percentage) {
    if (percentage < 0.0f || percentage > 1.0f) {
      throw new IllegalArgumentException(
          "Percentage must be between 0.0 and 1.0! Given " + percentage);
    }

    if (percentage == 1.0f) {
      return new Tuple<DenseDoubleMatrix, DenseDoubleMatrix>(this, null);
    } else if (percentage == 0.0f) {
      return new Tuple<DenseDoubleMatrix, DenseDoubleMatrix>(null, this);
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
    final double[][] firstMatrix = new double[firstMatrixRowsCount][numColumns];
    int firstMatrixIndex = 0;
    final double[][] secondMatrix = new double[numRows - firstMatrixRowsCount][numColumns];
    int secondMatrixIndex = 0;

    // then we loop over all items and put split the matrix
    for (int r = 0; r < numRows; r++) {
      if (lowerMatrixRowIndices.contains(r)) {
        firstMatrix[firstMatrixIndex++] = matrix[r];
      } else {
        secondMatrix[secondMatrixIndex++] = matrix[r];
      }
    }

    return new Tuple<DenseDoubleMatrix, DenseDoubleMatrix>(
        new DenseDoubleMatrix(firstMatrix), new DenseDoubleMatrix(secondMatrix));
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#multiply(double)
   */
  @Override
  public final DenseDoubleMatrix multiply(double scalar) {
    DenseDoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] * scalar);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#multiply(de.jungblut.math.DoubleMatrix)
   */
  @Override
  public final DoubleMatrix multiply(DoubleMatrix other) {
    DenseDoubleMatrix matrix = new DenseDoubleMatrix(this.getRowCount(),
        other.getColumnCount());

    final int m = this.numRows;
    final int n = this.numColumns;
    final int p = other.getColumnCount();

    for (int j = p; --j >= 0;) {
      for (int i = m; --i >= 0;) {
        double s = 0;
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
   * de.jungblut.math.DoubleMatrix#multiplyElementWise(de.jungblut.math.DoubleMatrix
   * )
   */
  @Override
  public final DoubleMatrix multiplyElementWise(DoubleMatrix other) {
    DenseDoubleMatrix matrix = new DenseDoubleMatrix(this.numRows,
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
   * de.jungblut.math.DoubleMatrix#multiplyVector(de.jungblut.math.DoubleVector)
   */
  @Override
  public final DoubleVector multiplyVector(DoubleVector v) {
    DoubleVector vector = new DenseDoubleVector(this.getRowCount());
    for (int row = 0; row < numRows; row++) {
      double sum = 0.0d;
      for (int col = 0; col < numColumns; col++) {
        sum += (matrix[row][col] * v.get(col));
      }
      vector.set(row, sum);
    }

    return vector;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#transpose()
   */
  @Override
  public DenseDoubleMatrix transpose() {
    DenseDoubleMatrix m = new DenseDoubleMatrix(this.numColumns, this.numRows);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(j, i, this.matrix[i][j]);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#subtractBy(double)
   */
  @Override
  public DenseDoubleMatrix subtractBy(double amount) {
    DenseDoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, amount - this.matrix[i][j]);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#subtract(double)
   */
  @Override
  public DenseDoubleMatrix subtract(double amount) {
    DenseDoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] - amount);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#subtract(de.jungblut.math.DoubleMatrix)
   */
  @Override
  public DoubleMatrix subtract(DoubleMatrix other) {
    DoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] - other.get(i, j));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#subtract(de.jungblut.math.DoubleVector)
   */
  @Override
  public DenseDoubleMatrix subtract(DoubleVector vec) {
    DenseDoubleMatrix cop = new DenseDoubleMatrix(this.getRowCount(),
        this.getColumnCount());
    for (int i = 0; i < this.getColumnCount(); i++) {
      cop.setColumn(i, getColumnVector(i).subtract(vec.get(i)).toArray());
    }
    return cop;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#divide(de.jungblut.math.DoubleVector)
   */
  @Override
  public DoubleMatrix divide(DoubleVector vec) {
    DoubleMatrix cop = new DenseDoubleMatrix(this.getRowCount(),
        this.getColumnCount());
    for (int i = 0; i < this.getColumnCount(); i++) {
      cop.setColumnVector(i, getColumnVector(i).divide(vec.get(i)));
    }
    return cop;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#divide(de.jungblut.math.DoubleMatrix)
   */
  @Override
  public DoubleMatrix divide(DoubleMatrix other) {
    DoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] / other.get(i, j));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#divide(double)
   */
  @Override
  public DoubleMatrix divide(double scalar) {
    DoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] / scalar);
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#add(de.jungblut.math.DoubleMatrix)
   */
  @Override
  public DoubleMatrix add(DoubleMatrix other) {
    DoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, this.matrix[i][j] + other.get(i, j));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#pow(int)
   */
  @Override
  public DoubleMatrix pow(int x) {
    DoubleMatrix m = new DenseDoubleMatrix(this.numRows, this.numColumns);
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        m.set(i, j, Math.pow(matrix[i][j], x));
      }
    }
    return m;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#max(int)
   */
  @Override
  public double max(int column) {
    double max = Double.MIN_VALUE;
    for (int i = 0; i < getRowCount(); i++) {
      double d = matrix[i][column];
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#min(int)
   */
  @Override
  public double min(int column) {
    double min = Double.MAX_VALUE;
    for (int i = 0; i < getRowCount(); i++) {
      double d = matrix[i][column];
      if (d < min) {
        min = d;
      }
    }
    return min;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#slice(int, int)
   */
  @Override
  public DoubleMatrix slice(int rows, int cols) {
    return slice(0, rows, 0, cols);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#slice(int, int, int, int)
   */
  @Override
  public DoubleMatrix slice(int rowOffset, int rowMax, int colOffset, int colMax) {
    DenseDoubleMatrix m = new DenseDoubleMatrix(rowMax - rowOffset, colMax
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
   * @see de.jungblut.math.DoubleMatrix#isSparse()
   */
  @Override
  public boolean isSparse() {
    return false;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#sum()
   */
  @Override
  public double sum() {
    double x = 0.0d;
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < numColumns; j++) {
        x += Math.abs(matrix[i][j]);
      }
    }
    return x;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleMatrix#columnIndices()
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
    DenseDoubleMatrix other = (DenseDoubleMatrix) obj;
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
  public static DenseDoubleMatrix eye(int dimension) {
    DenseDoubleMatrix m = new DenseDoubleMatrix(dimension, dimension);

    for (int i = 0; i < dimension; i++) {
      m.set(i, i, 1);
    }

    return m;
  }

  /**
   * Deep copies the given matrix into a new returned one.
   */
  public static DenseDoubleMatrix copy(DenseDoubleMatrix matrix) {
    final double[][] src = matrix.getValues();
    final double[][] dest = new double[matrix.getRowCount()][matrix
        .getColumnCount()];

    for (int i = 0; i < dest.length; i++)
      System.arraycopy(src[i], 0, dest[i], 0, src[i].length);

    return new DenseDoubleMatrix(dest);
  }

  /**
   * Some strange function I found in octave but I don't know what it was named.
   * It does however multiply the elements from the transposed vector and the
   * normal vector and sets it into the according indices of a new constructed
   * matrix.
   */
  public static DenseDoubleMatrix multiplyTransposedVectors(
      DoubleVector transposed, DoubleVector normal) {
    DenseDoubleMatrix m = new DenseDoubleMatrix(transposed.getLength(),
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
  public static double error(DenseDoubleMatrix a, DenseDoubleMatrix b) {
    return a.subtract(b).sum();
  }

}
