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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hama.commons.math.DoubleVector.DoubleVectorElement;
import org.junit.Test;

/**
 * Testcase for {@link DenseDoubleVector}
 *
 */
public class TestDenseDoubleVector {

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
  
  @Test
  public void testSliceNormal() {
    double[] arr1 = new double[] {2, 3, 4, 5, 6};
    double[] arr2 = new double[] {4, 5, 6};
    double[] arr3 = new double[] {2, 3, 4};
    DoubleVector vec1 = new DenseDoubleVector(arr1);
    assertArrayEquals(arr2, vec1.slice(2, 4).toArray(), 0.000001);
    DoubleVector vec2 = new DenseDoubleVector(arr1);
    assertArrayEquals(arr3, vec2.slice(3).toArray(), 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSliceAbnormal() {
    double[] arr1 = new double[] {2, 3, 4, 5, 6};
    DoubleVector vec = new DenseDoubleVector(arr1);
    vec.slice(2, 5);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSliceAbnormalEndTooLarge() {
    double[] arr1 = new double[] {2, 3, 4, 5, 6};
    DoubleVector vec = new DenseDoubleVector(arr1);
    vec.slice(2, 5);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSliceAbnormalStartLargerThanEnd() {
    double[] arr1 = new double[] {2, 3, 4, 5, 6};
    DoubleVector vec = new DenseDoubleVector(arr1);
    vec.slice(4, 3);
  }
  
  @Test
  public void testVectorMultiplyMatrix() {
    DoubleVector vec = new DenseDoubleVector(new double[]{1, 2, 3});
    DoubleMatrix mat = new DenseDoubleMatrix(new double[][] {
        {1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}
    });
    double[] expectedRes = new double[] {38, 44, 50, 56};
    
    assertArrayEquals(expectedRes, vec.multiply(mat).toArray(), 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testVectorMultiplyMatrixAbnormal() {
    DoubleVector vec = new DenseDoubleVector(new double[]{1, 2, 3});
    DoubleMatrix mat = new DenseDoubleMatrix(new double[][] {
        {1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}, {13, 14, 15, 16}
    });
    vec.multiply(mat);
  }
  
  @Test(timeout=100)
  public void testIterator() {
    double[] expectedRes = new double[] {38, 44, 50, 56, 0, 0, 3, 0, 0, 0};
    DoubleVector vec = new DenseDoubleVector(expectedRes);
    Iterator<DoubleVectorElement> itr = vec.iterate();
    
    int curIdx = 0;
    while (itr.hasNext()) {
      DoubleVectorElement elem = itr.next();
      assertEquals(curIdx, elem.getIndex());
      assertEquals(expectedRes[curIdx++], elem.getValue(), 0.000001);
    }
    
    Iterator<DoubleVectorElement> itrNonZero = vec.iterateNonDefault();
    
    curIdx = 0;
    while (itrNonZero.hasNext()) {
      while (expectedRes[curIdx] == 0.0) {
        ++curIdx;
      }
      assertEquals(expectedRes[curIdx++], itrNonZero.next().getValue(), 0.000001);
    }
  }
  
  @Test
  public void testApply() {
    double[] vals = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec = new DenseDoubleVector(vals);
    DoubleVector timesTwoVec = vec.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value * 3;
      }
      @Override
      public double applyDerivative(double value) {
        return 0;
      }
    });
    
    DoubleVector timesVec = timesTwoVec.applyToElements(vec, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return x1 * x2;
      }
      @Override
      public double applyDerivative(double x1, double x2) {
        return 0;
      }
    });
    
    for (int i = 0; i < vals.length; ++i) {
      assertEquals(vals[i] * 3, timesTwoVec.get(i), 0.000001);
      assertEquals(vals[i] * timesTwoVec.get(i), timesVec.get(i), 0.000001);
    }
  }
  
  @Test
  public void testAdd() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    
    DoubleVector vec3 = vec1.addUnsafe(vec2);
    
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(vec3.get(i), vec1.get(i) + vec2.get(i), 0.000001);
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testAddWithException() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1, 0};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    vec1.add(vec2);
  }
  
  @Test
  public void testSubtract() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    
    DoubleVector vec3 = vec1.subtractUnsafe(vec2);
    
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(vec3.get(i), vec1.get(i) - vec2.get(i), 0.000001);
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSubtractWithException() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1, 0};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    vec1.subtract(vec2);
  }
  
  @Test
  public void testSubtractFrom() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    double constant = 10;
    
    DoubleVector vec3 = vec1.subtractFrom(constant);
    
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(constant - vec1.get(i), vec3.get(i), 0.000001);
    }
  }
  
  @Test
  public void testMultiply() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    
    DoubleVector vec3 = vec1.multiplyUnsafe(vec2);
    
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(vec3.get(i), vec1.get(i) * vec2.get(i), 0.000001);
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testMultiplyWithException() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1, 0};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    vec1.multiply(vec2);
  }
  
  @Test
  public void testDivide() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    double constant = 10;
    
    DoubleVector vec3 = vec1.divide(constant);
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(vec1.get(i) / constant, vec3.get(i), 0.000001);
    }
  }
  
  @Test
  public void testPow() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    int constant = 5;
    
    DoubleVector vec3 = vec1.pow(constant);
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(Math.pow(vec1.get(i), 5), vec3.get(i), 0.000001);
    }
  }
  
  @Test
  public void testSqrt() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    
    DoubleVector vec3 = vec1.sqrt();
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(Math.sqrt(vec1.get(i)), vec3.get(i), 0.000001);
    }
  }
  
  @Test
  public void testSum() {
    double[] vals = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec = new DenseDoubleVector(vals);
    
    double expected = 0;
    double res = vec.sum();
    for (int i = 0; i < vals.length; ++i) {
      expected += vec.get(i);
    }
    assertEquals(expected, res, 0.000001);
  }
  
  @Test
  public void testAbs() {
    double[] vals = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec = new DenseDoubleVector(vals);
    
    DoubleVector vec2 = vec.abs();
    for (int i = 0; i < vals.length; ++i) {
      assertEquals(Math.abs(vec.get(i)), vec2.get(i), 0.000001);
    }
  }
  
  @Test
  public void testDivideFrom() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    double constant = 10;
    
    DoubleVector vec3 = vec1.divideFrom(constant);
    
    for (int i = 0; i < vals1.length; ++i) {
      assertEquals(constant / vec1.get(i), vec3.get(i), 0.000001);
    }
  }
  
  @Test
  public void testDot() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    
    double expected = 0.0;
    double res = vec1.dotUnsafe(vec2);
    
    for (int i = 0; i < vals1.length; ++i) {
      expected += vec1.get(i) * vec2.get(i);
    }
    assertEquals(expected, res, 0.000001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testDotWithException() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1, 0};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    vec1.dot(vec2);
  }
  
  @Test
  public void testSlice() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {2, 3, 4, 5, 6};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    
    DoubleVector vec3 = vec1.sliceUnsafe(1, 5);
    assertEquals(vec2, vec3);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSliceWithNegativeIndices() {
    double[] vals = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec = new DenseDoubleVector(vals);
    vec.slice(-1, -9);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSliceWithOutofBounds() {
    double[] vals = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec = new DenseDoubleVector(vals);
    vec.slice(1, 9);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testSliceWithNegativeLength() {
    double[] vals1 = {1, 2, 3, 4, 5, 6, 7, 8};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    vec1.slice(3, 2);
  }
  
  @Test
  public void testMinMax() {
    double[] vals = {11, 2, 3, 4, 15, 6, 7, 8};
    DoubleVector vec = new DenseDoubleVector(vals);
    assertEquals(15, vec.max(), 0.000001);
    assertEquals(2, vec.min(), 0.000001);
  }
  
  @Test
  public void testMaxIndexMinIndex() {
    double[] vals = {11, 2, 3, 4, 15, 6, 7, 8};
    DenseDoubleVector vec = new DenseDoubleVector(vals);
    assertEquals(4, vec.maxIndex());
    assertEquals(1, vec.minIndex());
  }
  
  @Test
  public void testRint() {
    double[] vals = {11, 2, 3, 4, 15, 6, 7, 8};
    DenseDoubleVector vec = new DenseDoubleVector(vals);
    DenseDoubleVector vec2 = vec.rint();
    for (int i = 0; i < vec.getDimension(); ++i) {
      assertEquals(Math.rint(vec.get(i)), vec2.get(i), 0.000001);
    }
  }
  
  @Test
  public void testRound() {
    double[] vals = {11, 2, 3, 4, 15, 6, 7, 8};
    DenseDoubleVector vec = new DenseDoubleVector(vals);
    DenseDoubleVector vec2 = vec.round();
    for (int i = 0; i < vec.getDimension(); ++i) {
      assertEquals(Math.round(vec.get(i)), vec2.get(i), 0.000001);
    }
  }
  
  @Test
  public void testCeil() {
    double[] vals = {11, 2, 3, 4, 15, 6, 7, 8};
    DenseDoubleVector vec = new DenseDoubleVector(vals);
    DenseDoubleVector vec2 = vec.ceil();
    for (int i = 0; i < vec.getDimension(); ++i) {
      assertEquals(Math.ceil(vec.get(i)), vec2.get(i), 0.000001);
    }
  }
  
  @Test
  public void testToArray() {
    double[] vals = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    DenseDoubleVector vec = new DenseDoubleVector(vals);
    double[] vals2 = vec.toArray();
    assertEquals(vals, vals2);
  }
  
  @Test(expected = AssertionError.class)
  public void deepCopy() {
    double[] vals1 = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    double[] vals2 = {8, 7, 6, 5, 4, 3, 2, 1, 0};
    DoubleVector vec1 = new DenseDoubleVector(vals1);
    DoubleVector vec2 = new DenseDoubleVector(vals2);
    
    DoubleVector vec3 = vec1.deepCopy();
    vec1 = vec1.add(vec2);
    assertEquals(vec1, vec3);
  }
  
  @Test
  public void testOnes() {
    DoubleVector vec = DenseDoubleVector.ones(10);
    for (int i = 0; i < vec.getDimension(); ++i) {
      assertEquals(1, vec.get(i), 0.000001);
    }
  }
  
  @Test
  public void testFromUpTo() {
    double from = 11;
    double to = 111.5;
    double stepsize = 2.5;
    
    DoubleVector vec = DenseDoubleVector.fromUpTo(from, to, stepsize);
    
    int curIndex = 0;
    double cur = 11;
    while (cur <= to) {
      assertEquals(cur, vec.get(curIndex), 0.000001);
      cur += stepsize;
      ++curIndex;
    }
    
  }
  
  @Test
  public void testSort() {
    double[] vals = {12, 32, 31, 11, 52, 13, -1, -222, 2};
    DoubleVector vec = new DenseDoubleVector(vals);
    
    Comparator<Double> comparator = new Comparator<Double>() {
      @Override
      public int compare(Double arg0, Double arg1) {
        return Double.compare(arg0, arg1);
      }
    };
    
    List<Tuple<Double, Integer>> sorted = DenseDoubleVector.sort(vec, comparator);
    for (int i = 1; i < sorted.size(); ++i) {
      Tuple<Double, Integer> previous = sorted.get(i - 1);
      Tuple<Double, Integer> cur = sorted.get(i);
      assertTrue(previous.getFirst() <= cur.getFirst());
    }
    
  }
  
}
