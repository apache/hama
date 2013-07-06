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
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Testcase for {@link DenseDoubleVector}
 *
 */
public class TestDenseDoubleVector {

  @Test
  public void testApplyDoubleFunction() {
    double[] values = new double[] {1, 2, 3, 4, 5};
    double[] result = new double[] {2, 3, 4, 5, 6};
    
    DoubleVector vec1 = new DenseDoubleVector(values);
    
    vec1.applyToElements(new DoubleFunction() {

      @Override
      public double apply(double value) {
        return value + 1;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException("Not supported.");
      }
      
    });
    
    assertArrayEquals(result, vec1.toArray(), 0.0001);
  }
  
  @Test
  public void testApplyDoubleDoubleFunction() {
    double[] values1 = new double[] {1, 2, 3, 4, 5, 6};
    double[] values2 = new double[] {7, 8, 9, 10, 11, 12};
    double[] result = new double[] {8, 10, 12, 14, 16, 18};
    
    DoubleVector vec1 = new DenseDoubleVector(values1);
    DoubleVector vec2 = new DenseDoubleVector(values2);
    
    vec1.applyToElements(vec2, new DoubleDoubleFunction() {

      @Override
      public double apply(double x1, double x2) {
        return x1 + x2;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        throw new UnsupportedOperationException("Not supported");
      }
      
    });
    
    assertArrayEquals(result, vec1.toArray(), 0.0001);
    
  }
  
  @Test
  public void testAddNormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5, 6};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    double[] arrExp = new double[] {5, 7, 9};
    assertArrayEquals(arrExp, vec1.add(vec2).toArray(), 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testAddAbnormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    vec1.add(vec2);
  }
  
  @Test
  public void testSubtractNormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5, 6};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    double[] arrExp = new double[] {-3, -3, -3};
    assertArrayEquals(arrExp, vec1.subtract(vec2).toArray(), 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSubtractAbnormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    vec1.subtract(vec2);
  }
  
  @Test
  public void testMultiplyNormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5, 6};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    double[] arrExp = new double[] {4, 10, 18};
    assertArrayEquals(arrExp, vec1.multiply(vec2).toArray(), 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testMultiplyAbnormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    vec1.multiply(vec2);
  }
  
  @Test
  public void testDotNormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5, 6};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    assertEquals(32.0, vec1.dot(vec2), 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testDotAbnormal() {
    double[] arr1 = new double[] {1, 2, 3};
    double[] arr2 = new double[] {4, 5};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    DoubleVector vec2 = new DenseDoubleVector(arr2);
    vec1.add(vec2);
  }
}
