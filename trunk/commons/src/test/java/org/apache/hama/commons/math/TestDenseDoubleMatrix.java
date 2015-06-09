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

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

/**
 * Test case for {@link DenseDoubleMatrix}
 * 
 */
public class TestDenseDoubleMatrix {

  @Test
  public void testDoubleFunction() {
    double[][] values = new double[][] { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } };

    double[][] result = new double[][] { { 2, 3, 4 }, { 5, 6, 7 }, { 8, 9, 10 } };

    DenseDoubleMatrix mat = new DenseDoubleMatrix(values);
    mat.applyToElements(new DoubleFunction() {

      @Override
      public double apply(double value) {
        return value + 1;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }

    });

    double[][] actual = mat.getValues();
    for (int i = 0; i < actual.length; ++i) {
      assertArrayEquals(result[i], actual[i], 0.0001);
    }
  }

  @Test
  public void testDoubleDoubleFunction() {
    double[][] values1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } };
    double[][] values2 = new double[][] { { 2, 3, 4 }, { 5, 6, 7 },
        { 8, 9, 10 } };
    double[][] result = new double[][] { { 3, 5, 7 }, { 9, 11, 13 },
        { 15, 17, 19 } };

    DenseDoubleMatrix mat1 = new DenseDoubleMatrix(values1);
    DenseDoubleMatrix mat2 = new DenseDoubleMatrix(values2);

    mat1.applyToElements(mat2, new DoubleDoubleFunction() {

      @Override
      public double apply(double x1, double x2) {
        return x1 + x2;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        throw new UnsupportedOperationException();
      }

    });

    double[][] actual = mat1.getValues();
    for (int i = 0; i < actual.length; ++i) {
      assertArrayEquals(result[i], actual[i], 0.0001);
    }
  }

  @Test
  public void testMultiplyNormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[][] mat2 = new double[][] { { 6, 5 }, { 4, 3 }, { 2, 1 } };
    double[][] expMat = new double[][] { { 20, 14 }, { 56, 41 } };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    DoubleMatrix actMatrix = matrix1.multiply(matrix2);
    for (int r = 0; r < actMatrix.getRowCount(); ++r) {
      assertArrayEquals(expMat[r], actMatrix.getRowVector(r).toArray(),
          0.000001);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiplyAbnormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[][] mat2 = new double[][] { { 6, 5 }, { 4, 3 } };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    matrix1.multiply(matrix2);
  }

  @Test
  public void testMultiplyElementWiseNormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[][] mat2 = new double[][] { { 6, 5, 4 }, { 3, 2, 1 } };
    double[][] expMat = new double[][] { { 6, 10, 12 }, { 12, 10, 6 } };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    DoubleMatrix actMatrix = matrix1.multiplyElementWise(matrix2);
    for (int r = 0; r < actMatrix.getRowCount(); ++r) {
      assertArrayEquals(expMat[r], actMatrix.getRowVector(r).toArray(),
          0.000001);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiplyElementWiseAbnormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[][] mat2 = new double[][] { { 6, 5 }, { 4, 3 } };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    matrix1.multiplyElementWise(matrix2);
  }

  @Test
  public void testMultiplyVectorNormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[] mat2 = new double[] { 6, 5, 4 };
    double[] expVec = new double[] { 28, 73 };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleVector vector2 = new DenseDoubleVector(mat2);
    DoubleVector actVec = matrix1.multiplyVector(vector2);
    assertArrayEquals(expVec, actVec.toArray(), 0.000001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiplyVectorAbnormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[] vec2 = new double[] { 6, 5 };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleVector vector2 = new DenseDoubleVector(vec2);
    matrix1.multiplyVector(vector2);
  }

  @Test
  public void testSubtractNormal() {
    double[][] mat1 = new double[][] {
        {1, 2, 3},
        {4, 5, 6}
    };
    double[][] mat2 = new double[][] {
        {6, 5, 4},
        {3, 2, 1}
    };
    double[][] expMat = new double[][] {
        {-5, -3, -1},
        {1, 3, 5}
    };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    DoubleMatrix actMatrix = matrix1.subtract(matrix2);
    for (int r = 0; r < actMatrix.getRowCount(); ++r) {
      assertArrayEquals(expMat[r], actMatrix.getRowVector(r).toArray(), 0.000001);
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSubtractAbnormal() {
    double[][] mat1 = new double[][] {
        {1, 2, 3},
        {4, 5, 6}
    };
    double[][] mat2 = new double[][] {
        {6, 5},
        {4, 3}
    };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    matrix1.subtract(matrix2);
  }
  
  @Test
  public void testDivideVectorNormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[] mat2 = new double[] { 6, 5, 4 };
    double[][] expVec = new double[][] { {1.0 / 6, 2.0 / 5, 3.0 / 4}, {4.0 / 6, 5.0 / 5, 6.0 / 4} };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleVector vector2 = new DenseDoubleVector(mat2);
    DoubleMatrix expMat = new DenseDoubleMatrix(expVec);
    DoubleMatrix actMat = matrix1.divide(vector2);
    for (int r = 0; r < actMat.getRowCount(); ++r) {
      assertArrayEquals(expMat.getRowVector(r).toArray(), actMat.getRowVector(r).toArray(), 0.000001);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDivideVectorAbnormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[] vec2 = new double[] { 6, 5 };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleVector vector2 = new DenseDoubleVector(vec2);
    matrix1.divide(vector2);
  }
  
  @Test
  public void testDivideNormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[][] mat2 = new double[][] { { 6, 5, 4 }, { 3, 2, 1 } };
    double[][] expMat = new double[][] { { 1.0 / 6, 2.0 / 5, 3.0 / 4 }, { 4.0 / 3, 5.0 / 2, 6.0 / 1 } };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    DoubleMatrix actMatrix = matrix1.divide(matrix2);
    for (int r = 0; r < actMatrix.getRowCount(); ++r) {
      assertArrayEquals(expMat[r], actMatrix.getRowVector(r).toArray(),
          0.000001);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDivideAbnormal() {
    double[][] mat1 = new double[][] { { 1, 2, 3 }, { 4, 5, 6 } };
    double[][] mat2 = new double[][] { { 6, 5 }, { 4, 3 } };
    DoubleMatrix matrix1 = new DenseDoubleMatrix(mat1);
    DoubleMatrix matrix2 = new DenseDoubleMatrix(mat2);
    matrix1.divide(matrix2);
  }
  
}
