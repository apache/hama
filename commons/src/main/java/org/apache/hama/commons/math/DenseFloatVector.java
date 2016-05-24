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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

public final class DenseFloatVector implements FloatVector {


  private float[] vector;

  public DenseFloatVector() { }
  
  /**
   * Creates a new vector with the given length.
   */
  public DenseFloatVector(int length) {
    this.vector = new float[length];
  }

  /**
   * Creates a new vector with the given length and default value.
   */
  public DenseFloatVector(int length, float val) {
    this(length);
    Arrays.fill(vector, val);
  }

  /**
   * Creates a new vector with the given array.
   */
  public DenseFloatVector(float[] arr) {
    this.vector = arr;
  }

  /**
   * Creates a new vector with the given array and the last value f1.
   */
  public DenseFloatVector(float[] array, float f1) {
    this.vector = new float[array.length + 1];
    System.arraycopy(array, 0, this.vector, 0, array.length);
    this.vector[array.length] = f1;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#get(int)
   */
  @Override
  public final float get(int index) {
    return vector[index];
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#getLength()
   */
  @Override
  public final int getLength() {
    return vector.length;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#getDimension()
   */
  @Override
  public int getDimension() {
    return getLength();
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#set(int, float)
   */
  @Override
  public final void set(int index, float value) {
    vector[index] = value;
  }

  /**
   * Apply a function to the element of the vector and returns a result vector.
   * Note that the function is applied on the copy of the original vector.
   */
  @Override
  public FloatVector applyToElements(FloatFunction func) {
    FloatVector newVec = new DenseFloatVector(this.getDimension());
    for (int i = 0; i < vector.length; i++) {
      newVec.set(i, func.apply(vector[i]));
    }
    return newVec;
  }

  /**
   * Apply a function to the element of the vector and another vector, and then returns a result vector.
   * Note that the function is applied on the copy of the original vectors.
   */
  @Override
  public FloatVector applyToElements(FloatVector other,
      FloatFloatFunction func) {
    FloatVector newVec = new DenseFloatVector(this.getDimension());
    for (int i = 0; i < vector.length; i++) {
      newVec.set(i, func.apply(vector[i], other.get(i)));
    }
    return newVec;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#add(de.jungblut.math.FloatVector)
   */
  @Override
  public final FloatVector addUnsafe(FloatVector v) {
    DenseFloatVector newv = new DenseFloatVector(v.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      newv.set(i, this.get(i) + v.get(i));
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#add(float)
   */
  @Override
  public final FloatVector add(float scalar) {
    FloatVector newv = new DenseFloatVector(this.getLength());
    for (int i = 0; i < this.getLength(); i++) {
      newv.set(i, this.get(i) + scalar);
    }
    return newv;
  }

  @Override
  public final FloatVector subtractUnsafe(FloatVector v) {
    FloatVector newv = new DenseFloatVector(v.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      newv.set(i, this.get(i) - v.get(i));
    }
    return newv;
  }

  @Override
  public final FloatVector subtract(float v) {
    DenseFloatVector newv = new DenseFloatVector(vector.length);
    for (int i = 0; i < vector.length; i++) {
      newv.set(i, vector[i] - v);
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#subtractFrom(float)
   */
  @Override
  public final FloatVector subtractFrom(float v) {
    DenseFloatVector newv = new DenseFloatVector(vector.length);
    for (int i = 0; i < vector.length; i++) {
      newv.set(i, v - vector[i]);
    }
    return newv;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#multiply(float)
   */
  @Override
  public FloatVector multiply(float scalar) {
    FloatVector v = new DenseFloatVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, this.get(i) * scalar);
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#multiply(de.jungblut.math.FloatVector)
   */
  @Override
  public FloatVector multiplyUnsafe(FloatVector vector) {
    FloatVector v = new DenseFloatVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, this.get(i) * vector.get(i));
    }
    return v;
  }

  @Override
  public FloatVector multiply(FloatMatrix matrix) {
    Preconditions.checkArgument(this.vector.length == matrix.getRowCount(),
        "Dimension mismatch when multiply a vector to a matrix.");
    return this.multiplyUnsafe(matrix);
  }

  @Override
  public FloatVector multiplyUnsafe(FloatMatrix matrix) {
    FloatVector vec = new DenseFloatVector(matrix.getColumnCount());
    for (int i = 0; i < vec.getDimension(); ++i) {
      vec.set(i, this.multiplyUnsafe(matrix.getColumnVector(i)).sum());
    }
    return vec;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#divide(float)
   */
  @Override
  public FloatVector divide(float scalar) {
    DenseFloatVector v = new DenseFloatVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, this.get(i) / scalar);
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#pow(int)
   */
  @Override
  public FloatVector pow(int x) {
    DenseFloatVector v = new DenseFloatVector(getLength());
    for (int i = 0; i < v.getLength(); i++) {
      float value = 0.0f;
      // it is faster to multiply when we having ^2
      if (x == 2) {
        value = vector[i] * vector[i];
      } else {
        value = (float) Math.pow(vector[i], x);
      }
      v.set(i, value);
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#sqrt()
   */
  @Override
  public FloatVector sqrt() {
    FloatVector v = new DenseFloatVector(getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, (float) Math.sqrt(vector[i]));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#sum()
   */
  @Override
  public float sum() {
    float sum = 0.0f;
    for (float aVector : vector) {
      sum += aVector;
    }
    return sum;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#abs()
   */
  @Override
  public FloatVector abs() {
    FloatVector v = new DenseFloatVector(getLength());
    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, Math.abs(vector[i]));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#divideFrom(float)
   */
  @Override
  public FloatVector divideFrom(float scalar) {
    FloatVector v = new DenseFloatVector(this.getLength());
    for (int i = 0; i < v.getLength(); i++) {
      if (this.get(i) != 0.0d) {
        float result = scalar / this.get(i);
        v.set(i, result);
      } else {
        v.set(i, 0.0f);
      }
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#dot(de.jungblut.math.FloatVector)
   */
  @Override
  public float dotUnsafe(FloatVector vector) {
    BigDecimal dotProduct = BigDecimal.valueOf(0.0d);
    for (int i = 0; i < getLength(); i++) {
      dotProduct = dotProduct.add(BigDecimal.valueOf(this.get(i)).multiply(BigDecimal.valueOf(vector.get(i))));
    }
    return dotProduct.floatValue();
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#slice(int)
   */
  @Override
  public FloatVector slice(int length) {
    return slice(0, length - 1);
  }

  @Override
  public FloatVector sliceUnsafe(int length) {
    return sliceUnsafe(0, length - 1);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#slice(int, int)
   */
  @Override
  public FloatVector slice(int start, int end) {
    Preconditions.checkArgument(start >= 0 && start <= end
        && end < vector.length, "The given from and to is invalid");

    return sliceUnsafe(start, end);
  }

  /**
   * Get a subset of the original vector starting from 'start' and end to 'end', 
   * with both ends inclusive.
   */
  @Override
  public FloatVector sliceUnsafe(int start, int end) {
    FloatVector newVec = new DenseFloatVector(end - start + 1);
    for (int i = start, j = 0; i <= end; ++i, ++j) {
      newVec.set(j, vector[i]);
    }

    return newVec;
  }

  /*
   * Return the maximum.
   */
  @Override
  public float max() {
    float max = -Float.MAX_VALUE;
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      if (d > max) {
        max = d;
      }
    }
    return max;
  }

  /**
   * Return the index of the first maximum.
   * @return the index where the maximum resides.
   */
  public int maxIndex() {
    float max = -Float.MAX_VALUE;
    int maxIndex = 0;
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      if (d > max) {
        max = d;
        maxIndex = i;
      }
    }
    return maxIndex;
  }

  /*
   * Return the minimum.
   */
  @Override
  public float min() {
    float min = Float.MAX_VALUE;
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      if (d < min) {
        min = d;
      }
    }
    return min;
  }

  /**
   * Return the index of the first minimum.
   * @return the index where the minimum resides.
   */
  public int minIndex() {
    float min = Float.MAX_VALUE;
    int minIndex = 0;
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      if (d < min) {
        min = d;
        minIndex = i;
      }
    }
    return minIndex;
  }

  /**
   * Round each of the element in the vector to the integer.
   * @return a new vector which has rinted each element.
   */
  public DenseFloatVector rint() {
    DenseFloatVector v = new DenseFloatVector(getLength());
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      v.set(i, (float) Math.rint(d));
    }
    return v;
  }

  /**
   * @return a new vector which has rounded each element.
   */
  public DenseFloatVector round() {
    DenseFloatVector v = new DenseFloatVector(getLength());
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      v.set(i, Math.round(d));
    }
    return v;
  }

  /**
   * @return a new vector which has ceiled each element.
   */
  public DenseFloatVector ceil() {
    DenseFloatVector v = new DenseFloatVector(getLength());
    for (int i = 0; i < getLength(); i++) {
      float d = vector[i];
      v.set(i, (float) Math.ceil(d));
    }
    return v;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#toArray()
   */
  @Override
  public final float[] toArray() {
    return vector;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#isSparse()
   */
  @Override
  public boolean isSparse() {
    return false;
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#deepCopy()
   */
  @Override
  public FloatVector deepCopy() {
    final float[] src = vector;
    final float[] dest = new float[vector.length];
    System.arraycopy(src, 0, dest, 0, vector.length);
    return new DenseFloatVector(dest);
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#iterateNonZero()
   */
  @Override
  public Iterator<FloatVectorElement> iterateNonDefault() {
    return new NonDefaultIterator();
  }

  /*
   * (non-Javadoc)
   * @see de.jungblut.math.FloatVector#iterate()
   */
  @Override
  public Iterator<FloatVectorElement> iterate() {
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
    DenseFloatVector other = (DenseFloatVector) obj;
    return Arrays.equals(vector, other.vector);
  }

  /**
   * Non-default iterator for vector elements.
   */
  private final class NonDefaultIterator extends
      AbstractIterator<FloatVectorElement> {

    private final FloatVectorElement element = new FloatVectorElement();
    private final float[] array;
    private int currentIndex = 0;

    private NonDefaultIterator() {
      this.array = vector;
    }

    @Override
    protected final FloatVectorElement computeNext() {
      if (currentIndex >= array.length) {
        return endOfData();
      }
      while (array[currentIndex] == 0.0d) {
        currentIndex++;
        if (currentIndex >= array.length)
          return endOfData();
      }
      element.setIndex(currentIndex);
      element.setValue(array[currentIndex]);
      ++currentIndex;
      return element;
    }
  }

  /**
   * Iterator for all elements.
   */
  private final class DefaultIterator extends
      AbstractIterator<FloatVectorElement> {

    private final FloatVectorElement element = new FloatVectorElement();
    private final float[] array;
    private int currentIndex = 0;

    private DefaultIterator() {
      this.array = vector;
    }

    @Override
    protected final FloatVectorElement computeNext() {
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
   * Generate a vector with all element to be 1.
   * @return a new vector with dimension num and a default value of 1.
   */
  public static DenseFloatVector ones(int num) {
    return new DenseFloatVector(num, 1.0f);
  }

  /**
   * Generate a vector whose elements are in increasing order,
   * where the start value is 'from', end value is 'to', with increment 'stepsize'.
   * @return a new vector filled from index, to index, with a given stepsize.
   */
  public static DenseFloatVector fromUpTo(float from, float to,
      float stepsize) {
    DenseFloatVector v = new DenseFloatVector(
        (int) (Math.round(((to - from) / stepsize) + 0.5)));

    for (int i = 0; i < v.getLength(); i++) {
      v.set(i, from + i * stepsize);
    }
    return v;
  }

  /**
   * Some crazy sort function.
   */
  public static List<Tuple<Float, Integer>> sort(FloatVector vector,
      final Comparator<Float> scoreComparator) {
    List<Tuple<Float, Integer>> list = new ArrayList<Tuple<Float, Integer>>(
        vector.getLength());
    for (int i = 0; i < vector.getLength(); i++) {
      list.add(new Tuple<Float, Integer>(vector.get(i), i));
    }
    Collections.sort(list, new Comparator<Tuple<Float, Integer>>() {
      @Override
      public int compare(Tuple<Float, Integer> o1, Tuple<Float, Integer> o2) {
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

  /*
   * (non-Javadoc)
   * @see org.apache.hama.ml.math.FloatVector#safeAdd(org.apache.hama.ml.math.
   * FloatVector)
   */
  @Override
  public FloatVector add(FloatVector vector) {
    Preconditions.checkArgument(this.vector.length == vector.getDimension(),
        "Dimensions of two vectors do not equal.");
    return this.addUnsafe(vector);
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.ml.math.FloatVector#safeSubtract(org.apache.hama.ml.math
   * .FloatVector)
   */
  @Override
  public FloatVector subtract(FloatVector vector) {
    Preconditions.checkArgument(this.vector.length == vector.getDimension(),
        "Dimensions of two vectors do not equal.");
    return this.subtractUnsafe(vector);
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.ml.math.FloatVector#safeMultiplay(org.apache.hama.ml.math
   * .FloatVector)
   */
  @Override
  public FloatVector multiply(FloatVector vector) {
    Preconditions.checkArgument(this.vector.length == vector.getDimension(),
        "Dimensions of two vectors do not equal.");
    return this.multiplyUnsafe(vector);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.ml.math.FloatVector#safeDot(org.apache.hama.ml.math.
   * FloatVector)
   */
  @Override
  public float dot(FloatVector vector) {
    Preconditions.checkArgument(this.vector.length == vector.getDimension(),
        "Dimensions of two vectors do not equal.");
    return this.dotUnsafe(vector);
  }

}
