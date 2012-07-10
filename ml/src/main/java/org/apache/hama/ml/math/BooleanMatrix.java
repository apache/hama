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

/**
 * A matrix consisting of booleans.
 */
public interface BooleanMatrix {

    /**
     * double conversion value for non-default element value.
     */
    public static final double NOT_FLAGGED = 0.0d;

    /**
     * Get a specific value of the matrix.
     *
     * @return the integer value at in the column at the row.
     */
    public boolean get(int row, int col);

    /**
     * Returns the number of columns in the matrix.
     */
    public int getColumnCount();

    /**
     * Get a whole column of the matrix as vector. If the specified column doesn't
     * exist a IllegalArgumentException is thrown.
     */
    public BooleanVector getColumnVector(int col);

    /**
     * Returns the number of rows in this matrix.
     */
    public int getRowCount();

    /**
     * Get a single row of the matrix as a vector.
     */
    public BooleanVector getRowVector(int row);

    /**
     * Sets the value at the given row and column index.
     */
    public void set(int row, int col, boolean value);

    /**
     * Transposes this matrix.
     */
    public BooleanMatrix transpose();

    /**
     * Returns an array of column indices existing in this matrix.
     */
    public int[] columnIndices();

}