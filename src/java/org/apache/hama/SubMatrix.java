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

import org.apache.log4j.Logger;

public class SubMatrix {
  static final Logger LOG = Logger.getLogger(SubMatrix.class);
  private double[][] matrix;
  
  public SubMatrix(int i, int j) {
    matrix = new double[i][j];
  }

  public SubMatrix(double[][] c) {
    double[][] matrix = c;
    this.matrix = matrix;
  }

  public void set(int row, int column, double value) {
    matrix[row][column] = value;
  }

  public double get(int i, int j) {
    return matrix[i][j];
  }

  public SubMatrix mult(SubMatrix b) {
    double[][] C = new double[size()][size()];
    for (int i = 0; i < size(); i++) {
      for (int j = 0; j < size(); j++) {
        for (int k = 0; k < size(); k++) {
          C[i][k] += this.get(i, j) * b.get(j, k);
        }
      }
    }

    return new SubMatrix(C);
  }
  
  public int size() {
    return matrix.length;
  }

  public void close() {
    matrix = null;
  }
}
