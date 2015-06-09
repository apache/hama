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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;

import org.apache.hama.commons.math.DoubleVector.DoubleVectorElement;
import org.junit.Test;

/**
 * The test cases of {@link SparseDoubleVector}.
 * 
 */
public class TestSparseDoubleVector {
  
  

  @Test
  public void testBasic() {
    DoubleVector v1 = new SparseDoubleVector(10);
    for (int i = 0; i < 10; ++i) {
      assertEquals(v1.get(i), 0.0, 0.000001);
    }

    DoubleVector v2 = new SparseDoubleVector(10, 2.5);
    for (int i = 0; i < 10; ++i) {
      assertEquals(v2.get(i), 2.5, 0.000001);
    }

    assertEquals(v1.getDimension(), 10);
    assertEquals(v2.getLength(), 10);

    v1.set(5, 2);
    assertEquals(v1.get(5), 2, 0.000001);
  }

  @Test
  public void testIterators() {
    DoubleVector v1 = new SparseDoubleVector(10, 5.5);
    Iterator<DoubleVectorElement> itr1 = v1.iterate();
    int idx1 = 0;
    while (itr1.hasNext()) {
      DoubleVectorElement elem = itr1.next();
      assertEquals(idx1++, elem.getIndex());
      assertEquals(5.5, elem.getValue(), 0.000001);
    }

    v1.set(2, 20);
    v1.set(6, 30);

    Iterator<DoubleVectorElement> itr2 = v1.iterateNonDefault();
    DoubleVectorElement elem = itr2.next();
    assertEquals(2, elem.getIndex());
    assertEquals(20, elem.getValue(), 0.000001);
    elem = itr2.next();
    assertEquals(6, elem.getIndex());
    assertEquals(30, elem.getValue(), 0.000001);

    assertFalse(itr2.hasNext());
  }

  @Test
  public void testApplyToElements() {
    // v1 = {5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5, 5.5}
    DoubleVector v1 = new SparseDoubleVector(10, 5.5);

    // v2 = {60.6, 60.5, 60.5, 60.5, 60.5, 60.5, 60.5, 60.5, 60.5, 60.5}
    DoubleVector v2 = v1.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value * 11;
      }

      @Override
      public double applyDerivative(double value) {
        return 0;
      }
    });

    // v3 = {4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5}
    DoubleVector v3 = v1.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value / 2 + 1.75;
      }

      @Override
      public double applyDerivative(double value) {
        return 0;
      }
    });

    // v4 = {66, 66, 66, 66, 66, 66, 66, 66, 66, 66}
    DoubleVector v4 = v1.applyToElements(v2, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return x1 + x2;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        return 0;
      }
    });

    for (int i = 0; i < 10; ++i) {
      assertEquals(v1.get(i), 5.5, 0.000001);
      assertEquals(v2.get(i), 60.5, 0.000001);
      assertEquals(v3.get(i), 4.5, 0.000001);
      assertEquals(v4.get(i), 66, 0.000001);
    }

    // v3 = {4.5, 4.5, 4.5, 10, 4.5, 4.5, 10, 4.5, 200, 4.5}
    v3.set(3, 10);
    v3.set(6, 10);
    v3.set(8, 200);

    // v4 = {66, 66, 66, 66, 66, 100, 66, 66, 1, 66}
    v4.set(5, 100);
    v4.set(8, 1);

    // v5 = {615, 615, 615, 560, 615, 955, 560, 615, -1990, 615}
    DoubleVector v5 = v4.applyToElements(v3, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return (x1 - x2) * 10;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        return 0;
      }
    });

    // v6 = {615, 615, 615, 560, 615, 955, 560, 615, -1990, 615}
    DoubleVector v6 = new SparseDoubleVector(10, 615);
    v6.set(3, 560);
    v6.set(5, 955);
    v6.set(6, 560);
    v6.set(8, -1990);

    for (int i = 0; i < v5.getDimension(); ++i) {
      assertEquals(v5.get(i), v6.get(i), 0.000001);
    }

    // v7 = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}
    DoubleVector v7 = new DenseDoubleVector(new double[] { 0.0, 1.0, 2.0, 3.0,
        4.0, 5.0, 6.0, 7.0, 8.0, 9.0 });

    DoubleVector v8 = v5.applyToElements(v7, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return (x1 + x2) * 3.3;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        return 0;
      }
    });

    DoubleVector v9 = v6.applyToElements(v7, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return (x1 + x2) * 3.3;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        return 0;
      }
    });

    for (int i = 0; i < v7.getDimension(); ++i) {
      assertEquals(v8.get(i), v9.get(i), 0.000001);
    }

  }

  @Test
  public void testAdd() {
    // addition of two sparse vectors
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10);
    for (int i = 0; i < spVec2.getDimension(); ++i) {
      spVec2.set(i, 1.5);
    }

    DoubleVector expRes1 = new SparseDoubleVector(10, 3.0);
    assertEquals(expRes1, spVec1.add(spVec2));

    // addition of one sparse vector and one dense vector
    DoubleVector dsVec1 = new DenseDoubleVector(10);
    for (int i = 0; i < dsVec1.getDimension(); ++i) {
      dsVec1.set(i, 1.5);
    }

    DoubleVector expRes2 = new DenseDoubleVector(10);
    for (int i = 0; i < expRes2.getDimension(); ++i) {
      expRes2.set(i, 3.0);
    }
    assertEquals(expRes2, dsVec1.add(spVec2));
  }

  @Test
  public void testSubtract() {
    // subtract two sparse vectors
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10);
    DoubleVector spVec3 = new SparseDoubleVector(10, 2.2);
    for (int i = 0; i < spVec2.getDimension(); ++i) {
      spVec2.set(i, 1.2);
    }
    DoubleVector expRes1 = new SparseDoubleVector(10, 0.3);
    assertEquals(expRes1, spVec1.subtract(spVec2));

    DoubleVector expRes2 = new SparseDoubleVector(10, -0.7);
    assertEquals(expRes2, spVec1.subtract(spVec3));

    // subtract one sparse vector from a dense vector
    DoubleVector dsVec1 = new DenseDoubleVector(10);
    for (int i = 0; i < dsVec1.getDimension(); ++i) {
      dsVec1.set(i, 1.7);
    }
    DoubleVector expRes3 = new SparseDoubleVector(10, 0.2);
    assertEquals(expRes3, dsVec1.subtract(spVec1));

    // subtract one dense vector from a sparse vector
    DoubleVector expRes4 = new SparseDoubleVector(10, -0.2);
    assertEquals(expRes4, spVec1.subtract(dsVec1));
  }

  @Test
  public void testMultiplyScala() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10);
    DoubleVector spVec3 = new SparseDoubleVector(10, 2.2);

    DoubleVector spRes1 = spVec1.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value * 3;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });

    assertEquals(spRes1, spVec1.multiply(3));
    assertEquals(spVec2, spVec2.multiply(1000));
    assertEquals(spVec2, spVec1.multiply(0));
    assertEquals(spVec1, spVec3.multiply(1.5 / 2.2));
  }

  @Test
  public void testMultiply() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10);
    DoubleVector spVec3 = new SparseDoubleVector(10, 2.2);

    DoubleVector spRes1 = spVec1.applyToElements(spVec3,
        new DoubleDoubleFunction() {
          @Override
          public double apply(double first, double second) {
            return first * second;
          }

          @Override
          public double applyDerivative(double value, double second) {
            throw new UnsupportedOperationException();
          }
        });

    assertEquals(spRes1, spVec1.multiply(spVec3));
    assertEquals(spVec2, spVec1.multiply(spVec2));
  }

  @Test
  public void testDivide() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10, 6.0);
    DoubleVector spVec3 = new SparseDoubleVector(10, 2.2);

    assertEquals(spVec3, spVec1.divide(1.5 / 2.2));
    assertEquals(spVec2, spVec1.divideFrom(9));
  }

  @Test
  public void testPow() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10, 1);
    DoubleVector spVec3 = new SparseDoubleVector(10, 2.25);

    assertEquals(spVec3, spVec1.pow(2));
    assertEquals(spVec2, spVec1.pow(0));
  }

  @Test
  public void testAbs() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec2 = new SparseDoubleVector(10, 0);
    DoubleVector spVec3 = new SparseDoubleVector(10, -1.5);

    assertEquals(spVec1, spVec3.abs());
    assertEquals(spVec2, spVec2.abs());
  }

  @Test
  public void testSqrt() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 2.25);
    DoubleVector spVec2 = new SparseDoubleVector(10, 0);
    DoubleVector spVec3 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec4 = new SparseDoubleVector(10, 1);

    assertEquals(spVec3, spVec1.sqrt());
    assertEquals(spVec2, spVec2.sqrt());
    assertEquals(spVec4, spVec4.sqrt());
  }

  @Test
  public void testSum() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 2.25);
    DoubleVector spVec2 = new SparseDoubleVector(10, 0);
    DoubleVector spVec3 = new SparseDoubleVector(10, 1.5);

    assertEquals(22.5, spVec1.sum(), 0.00001);
    assertEquals(0, spVec2.sum(), 0.000001);
    assertEquals(15, spVec3.sum(), 0.000001);
  }

  @Test
  public void testDot() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 2.25);
    DoubleVector spVec2 = new SparseDoubleVector(10, 0);
    DoubleVector spVec3 = new SparseDoubleVector(10, 1.5);
    DoubleVector spVec4 = new SparseDoubleVector(10, 1);

    assertEquals(spVec1.multiply(spVec3).sum(), spVec1.dot(spVec3), 0.000001);
    assertEquals(spVec3.sum(), spVec3.dot(spVec4), 0.000001);
    assertEquals(0, spVec1.dot(spVec2), 0.000001);
  }

  @Test
  public void testSlice() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 2.25);
    DoubleVector spVec2 = new SparseDoubleVector(10, 0);
    DoubleVector spVec3 = new SparseDoubleVector(5, 2.25);
    DoubleVector spVec4 = new SparseDoubleVector(5, 0);

    spVec1.set(7, 100);
    spVec2.set(2, 200);

    assertEquals(spVec3, spVec1.sliceUnsafe(5));
    assertFalse(spVec4.equals(spVec2.slice(5)));

    assertFalse(spVec3.equals(spVec1.slice(5, 9)));
    assertEquals(spVec4, spVec2.slice(5, 9));
  }

  @Test
  public void testMaxMin() {
    DoubleVector spVec1 = new SparseDoubleVector(10, 2.25);
    DoubleVector spVec2 = new SparseDoubleVector(10, 0);

    spVec1.set(7, 100);
    spVec2.set(2, 200);

    assertEquals(100, spVec1.max(), 0.000001);
    assertEquals(0, spVec2.min(), 0.000001);
  }

}
