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
import java.util.Random;

/**
 * A dense implementation of {@link BooleanMatrix}, internal representated as a
 * two-dimensional boolean array.
 */
public final class DenseBooleanMatrix implements BooleanMatrix {

    protected final boolean[][] matrix;
    protected final int numRows;
    protected final int numColumns;

    /**
     * Creates a new empty matrix of booleans with the number of rows and columns.
     *
     * @param rows the number of rows.
     * @param columns the number of columns.
     */
    public DenseBooleanMatrix(int rows, int columns) {
        this.numRows = rows;
        this.numColumns = columns;
        this.matrix = new boolean[rows][columns];
    }

    /**
     * Creates a new empty matrix of booleans with the number of rows and columns
     * with a defaultvalue.
     *
     * @param rows the number of rows.
     * @param columns the number of columns.
     * @param defaultValue default value.
     */
    public DenseBooleanMatrix(int rows, int columns, boolean defaultValue) {
        this.numRows = rows;
        this.numColumns = columns;
        this.matrix = new boolean[rows][columns];

        for (int i = 0; i < numRows; i++) {
            Arrays.fill(matrix[i], defaultValue);
        }
    }

    /**
     * Creates a new empty matrix of booleans with the number of rows and columns
     * with a random instance to fill the values.
     *
     * @param rows the number of rows.
     * @param columns the number of columns.
     * @param rand the random instance to use to fill this matrix.
     */
    public DenseBooleanMatrix(int rows, int columns, Random rand) {
        this.numRows = rows;
        this.numColumns = columns;
        this.matrix = new boolean[rows][columns];

        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numColumns; j++) {
                matrix[i][j] = rand.nextBoolean();
            }
        }
    }

    /**
     * Copy constructor, does make a shallow copy.
     *
     * @param otherMatrix a two dimensional other matrix.
     */
    public DenseBooleanMatrix(boolean[][] otherMatrix) {
        this.matrix = otherMatrix;
        this.numRows = otherMatrix.length;
        if (matrix.length > 0)
            this.numColumns = matrix[0].length;
        else
            this.numColumns = numRows;
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#get(int, int)
    */
    @Override
    public final boolean get(int row, int col) {
        return this.matrix[row][col];
    }

    /**
     * Get a whole column of the matrix as a boolean array.
     */
    public final boolean[] getColumn(int col) {
        final boolean[] column = new boolean[numRows];
        for (int r = 0; r < numRows; r++) {
            column[r] = matrix[r][col];
        }
        return column;
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#getColumnCount()
    */
    @Override
    public final int getColumnCount() {
        return numColumns;
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#getColumnVector(int)
    */
    @Override
    public final BooleanVector getColumnVector(int col) {
        return new DenseBooleanVector(getColumn(col));
    }

    /**
     * Get the matrix as 2-dimensional integer array (first index is the row,
     * second the column) to faster access the values.
     */
    public final boolean[][] getValues() {
        return matrix;
    }

    /**
     * Get a single row of the matrix as an integer array.
     */
    public final boolean[] getRow(int row) {
        return matrix[row];
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#getRowCount()
    */
    @Override
    public final int getRowCount() {
        return numRows;
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#getRowVector(int)
    */
    @Override
    public final BooleanVector getRowVector(int row) {
        return new DenseBooleanVector(getRow(row));
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#set(int, int, boolean)
    */
    @Override
    public final void set(int row, int col, boolean value) {
        this.matrix[row][col] = value;
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#transpose()
    */
    @Override
    public BooleanMatrix transpose() {
        DenseBooleanMatrix m = new DenseBooleanMatrix(this.numColumns, this.numRows);
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numColumns; j++) {
                m.set(j, i, this.matrix[i][j]);
            }
        }
        return m;
    }

    /**
     * Returns the size of the matrix as string (ROWSxCOLUMNS).
     */
    public String sizeToString() {
        return numRows + "x" + numColumns;
    }

    @Override
    public String toString() {
        return Arrays.deepToString(matrix);
    }

    /*
    * (non-Javadoc)
    * @see de.jungblut.math.BooleanMatrix#columnIndices()
    */
    @Override
    public int[] columnIndices() {
        int[] x = new int[getColumnCount()];
        for (int i = 0; i < getColumnCount(); i++)
            x[i] = i;
        return x;
    }

}