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
    double[][] values2 = new double[][] { { 2, 3, 4 }, { 5, 6, 7 }, { 8, 9, 10 } };
    double[][] result = new double[][] { {3, 5, 7}, {9, 11, 13}, {15, 17, 19}};

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

}
