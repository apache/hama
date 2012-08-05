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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;

/**
 * Dense double vector implementation.
 */
public final class DenseDoubleVector implements DoubleVector {

  private final double[] vector;

  /**
   * Creates a new vector with the given length.
   */
  public DenseDoubleVector(int length) {
    this.vector = new double[length];
  }

  /**
   * Creates a new vector with the given length and default value.
   */
  public DenseDoubleVector(int length, double val) {
    this(length);
    Arrays.fill(vector, val);
  }

  /**
   * Creates a new vector with the given array.
   */
  public DenseDoubleVector(double[] arr) {
    this.vector = arr;
  }

  /**
   * Creates a new vector with the given array and the last value f1.
   */
  public DenseDoubleVector(double[] array, double f1) {
    this.vector = new double[array.length + 1];
    System.arraycopy(array, 0, this.vector, 0, array.length);
    this.vector[array.length] = f1;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#get(int)
   */
  @Override
  public final double get(int index) {
    return vector[index];
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#getLength()
   */
  @Override
  public final int getLength() {
    return vector.length;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#getDimension()
   */
  @Override
  public int getDimension() {
    return getLength();
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#set(int, double)
   */
  @Override
  public final void set(int index, double value) {
    vector[index] = value;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#apply(de.jungblut.math.function.
   * DoubleVectorFunction)
   */
  @Override
  public DoubleVector apply(DoubleVectorFunction func) {
    DenseDoubleVector newV = new DenseDoubleVector(this.vector);
    for (int i = 0; i < vector.length; i++) {
      newV.vector[i] = func.calculate(i, vector[i]);
    }
    return newV;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#apply(de.jungblut.math.DoubleVector,
   * de.jungblut.math.function.DoubleDoubleVectorFunction)
   */
  @Override
  public DoubleVector apply(DoubleVector other, DoubleDoubleVectorFunction func) {
    DenseDoubleVector newV = (DenseDoubleVector) deepCopy();
    for (int i = 0; i < vector.length; i++) {
      newV.vector[i] = func.calculate(i, vector[i], other.get(i));
    }
    return newV;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#add(de.jungblut.math.DoubleVector)
   */
  @Override
  public final DoubleVector add(DoubleVector v) {
    DenseDoubleVector newv = new DenseDoubleVector(v.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      newv.set(i, this.get(i) + v.get(i));
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#add(double)
   */
  @Override
  public final DoubleVector add(double scalar) {
    DoubleVector newv = new DenseDoubleVector(this.getLength());
    for (int i = 0; i < this.getLength(); i++) {
      newv.set(i, this.get(i) + scalar);
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#subtract(de.jungblut.math.DoubleVector)
   */
  @Override
  public final DoubleVector subtract(DoubleVector v) {
    DoubleVector newv = new DenseDoubleVector(v.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      newv.set(i, this.get(i) - v.get(i));
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#subtract(double)
   */
  @Override
  public final DoubleVector subtract(double v) {
    DenseDoubleVector newv = new DenseDoubleVector(vector.length);
    for (int i = 0; i < vector.length; i++) {
      newv.set(i, vector[i] - v);
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#subtractFrom(double)
   */
  @Override
  public final DoubleVector subtractFrom(double v) {
    DenseDoubleVector newv = new DenseDoubleVector(vector.length);
    for (int i = 0; i < vector.length; i++) {
      newv.set(i, v - vector[i]);
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#multiply(double)
   */
  @Override
  public DoubleVector multiply(double scalar) {
    DoubleVector v = new DenseDoubleVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, this.get(i) * scalar);
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#multiply(de.jungblut.math.DoubleVector)
   */
  @Override
  public DoubleVector multiply(DoubleVector vector) {
    DoubleVector v = new DenseDoubleVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, this.get(i) * vector.get(i));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#divide(double)
   */
  @Override
  public DoubleVector divide(double scalar) {
    DenseDoubleVector v = new DenseDoubleVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, this.get(i) / scalar);
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#pow(int)
   */
  @Override
  public DoubleVector pow(int x) {
    DenseDoubleVector v = new DenseDoubleVector(getLength());
    for (int i = 0; i < v.getLength(); i++) {
      double value = 0.0d;
      // it is faster to multiply when we having ^2
      if (x == 2) {
        value = vector[i] * vector[i];
      } else {
        value = Math.pow(vector[i], x);
      }
      v.set(i, value);
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#sqrt()
   */
  @Override
  public DoubleVector sqrt() {
    DoubleVector v = new DenseDoubleVector(getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, Math.sqrt(vector[i]));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#sum()
   */
  @Override
  public double sum() {
    double sum = 0.0d;
    for (double aVector : vector) {
      sum += aVector;
    }
    return sum;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#abs()
   */
  @Override
  public DoubleVector abs() {
    DoubleVector v = new DenseDoubleVector(getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, Math.abs(vector[i]));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#divideFrom(double)
   */
  @Override
  public DoubleVector divideFrom(double scalar) {
    DoubleVector v = new DenseDoubleVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      if (this.get(i) != 0.0d) {
        double result = scalar / this.get(i);
        v.set(i, result);
      } else {
        v.set(i, 0.0d);
      }
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#dot(de.jungblut.math.DoubleVector)
   */
  @Override
  public double dot(DoubleVector s) {
    double dotProduct = 0.0d;
    for (int i = 0; i < getLength(); i++) {
      dotProduct += this.get(i) * s.get(i);
    }
    return dotProduct;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#slice(int)
   */
  @Override
  public DoubleVector slice(int length) {
    return slice(0, length);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#slice(int, int)
   */
  @Override
  public DoubleVector slice(int offset, int length) {
    DoubleVector nv = new DenseDoubleVector(length - offset);
    int index = 0;
    for (int i = offset; i < length; i++) {
      nv.set(index++, vector[i]);
    }

    return nv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#max()
   */
  @Override
  public double max() {
    double max = -Double.MAX_VALUE;
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  /**
   * @return the index where the maximum resides.
   */
  public int maxIndex() {
    double max = -Double.MAX_VALUE;
    int maxIndex = 0;
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      if (d > max) {
        max = d;
        maxIndex = i;
      }
    }
    return maxIndex;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#min()
   */
  @Override
  public double min() {
    double min = Double.MAX_VALUE;
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      if (d < min) {
        min = d;
      }
    }
    return min;
  }

  /**
   * @return the index where the minimum resides.
   */
  public int minIndex() {
    double min = Double.MAX_VALUE;
    int minIndex = 0;
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      if (d < min) {
        min = d;
        minIndex = i;
      }
    }
    return minIndex;
  }

  /**
   * @return a new vector which has rinted each element.
   */
  public DenseDoubleVector rint() {
    DenseDoubleVector v = new DenseDoubleVector(getLength());
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      v.set(i, Math.rint(d));
    }
    return v;
  }

  /**
   * @return a new vector which has rounded each element.
   */
  public DenseDoubleVector round() {
    DenseDoubleVector v = new DenseDoubleVector(getLength());
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      v.set(i, Math.round(d));
    }
    return v;
  }

  /**
   * @return a new vector which has ceiled each element.
   */
  public DenseDoubleVector ceil() {
    DenseDoubleVector v = new DenseDoubleVector(getLength());
    for (int i = 0; i < getLength(); i++) {
      double d = vector[i];
      v.set(i, Math.ceil(d));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#toArray()
   */
  @Override
  public final double[] toArray() {
    return vector;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#isSparse()
   */
  @Override
  public boolean isSparse() {
    return false;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#deepCopy()
   */
  @Override
  public DoubleVector deepCopy() {
    final double[] src = vector;
    final double[] dest = new double[vector.length];
    System.arraycopy(src, 0, dest, 0, vector.length);
    return new DenseDoubleVector(dest);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#iterateNonZero()
   */
  @Override
  public Iterator<DoubleVectorElement> iterateNonZero() {
    return new NonZeroIterator();
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.DoubleVector#iterate()
   */
  @Override
  public Iterator<DoubleVectorElement> iterate() {
    return new DefaultIterator();
  }

  @Override
  public final String toString() {
    if (getLength() < 20) {
      return Arrays.toString(vector);
    } else {
      return getLength() + "x1";
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(vector);
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
    DenseDoubleVector other = (DenseDoubleVector) obj;
    return Arrays.equals(vector, other.vector);
  }

  /**
   * Non-zero iterator for vector elements.
   */
  private final class NonZeroIterator extends
      AbstractIterator<DoubleVectorElement> {

    private final DoubleVectorElement element = new DoubleVectorElement();
    private final double[] array;
    private int currentIndex = 0;

    private NonZeroIterator() {
      this.array = vector;
    }

    @Override
    protected final DoubleVectorElement computeNext() {
      while (array[currentIndex] == 0.0d) {
        currentIndex++;
        if (currentIndex >= array.length)
          return endOfData();
      }
      element.setIndex(currentIndex);
      element.setValue(array[currentIndex]);
      return element;
    }
  }

  /**
   * Iterator for all elements.
   */
  private final class DefaultIterator extends
      AbstractIterator<DoubleVectorElement> {

    private final DoubleVectorElement element = new DoubleVectorElement();
    private final double[] array;
    private int currentIndex = 0;

    private DefaultIterator() {
      this.array = vector;
    }

    @Override
    protected final DoubleVectorElement computeNext() {
      if (currentIndex < array.length) {
        element.setIndex(currentIndex);
        element.setValue(array[currentIndex]);
        currentIndex++;
        return element;
      } else {
        return endOfData();
      }
    }

  }

  /**
   * @return a new vector with dimension num and a default value of 1.
   */
  public static DenseDoubleVector ones(int num) {
    return new DenseDoubleVector(num, 1.0d);
  }

  /**
   * @return a new vector filled from index, to index, with a given stepsize.
   */
  public static DenseDoubleVector fromUpTo(double from, double to,
      double stepsize) {
    DenseDoubleVector v = new DenseDoubleVector(
        (int) (Math.round(((to - from) / stepsize) + 0.5)));

    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, from + i * stepsize);
    }
    return v;
  }

  /**
   * Some crazy sort function.
   */
  public static List<Tuple<Double, Integer>> sort(DoubleVector vector,
      final Comparator<Double> scoreComparator) {
    List<Tuple<Double, Integer>> list = new ArrayList<Tuple<Double, Integer>>(
        vector.getLength());
    for (int i = 0; i < vector.getLength(); i++) {
      list.add(new Tuple<Double, Integer>(vector.get(i), i));
    }
    Collections.sort(list, new Comparator<Tuple<Double, Integer>>() {
      @Override
      public int compare(Tuple<Double, Integer> o1, Tuple<Double, Integer> o2) {
        return scoreComparator.compare(o1.getFirst(), o2.getFirst());
      }
    });
    return list;
  }

  @Override
  public boolean isNamed() {
    return false;
  }

  @Override
  public String getName() {
    return null;
  }

}
