/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hama.matrix;

import java.io.IOException;

class MatrixTestCommon {

  static double verifyNorm1(Matrix m1) throws IOException {
    double[] colSum = new double[m1.getColumns()];
    for (int j = 0; j < m1.getColumns(); j++) {
      for (int i = 0; i < m1.getRows(); i++) {
        colSum[j] += Math.abs(m1.get(i, j));
      }
    }

    double max = 0;
    for (int i = 0; i < colSum.length; i++) {
      max = Math.max(colSum[i], max);
    }
    return max;
  }

  static double verifyNormInfinity(Matrix m1) throws IOException {
    double[] rowSum = new double[m1.getRows()];
    for (int i = 0; i < m1.getRows(); i++) {
      for (int j = 0; j < m1.getColumns(); j++) {
        rowSum[i] += Math.abs(m1.get(i, j));
      }
    }

    double max = 0;
    for (int i = 0; i < rowSum.length; ++i)
      max = Math.max(rowSum[i], max);
    return max;
  }

  static double verifyNormMaxValue(Matrix m1) throws IOException {
    double max = 0;
    for (int i = 0; i < m1.getRows(); i++) {
      for (int j = 0; j < m1.getColumns(); j++) {
        max = Math.max(Math.abs(m1.get(i, j)), max);
      }
    }

    return max;
  }

  static double verifyNormFrobenius(Matrix m1) throws IOException {
    double sqrtSum = 0;
    for (int i = 0; i < m1.getRows(); i++) {
      for (int j = 0; j < m1.getColumns(); j++) {
        double cellValue = m1.get(i, j);
        sqrtSum += (cellValue * cellValue);
      }
    }
    return Math.sqrt(sqrtSum);
  }

}
