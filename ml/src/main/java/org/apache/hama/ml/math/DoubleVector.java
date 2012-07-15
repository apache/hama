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

import java.util.Iterator;

/**
 * Vector with doubles. Some of the operations are mutable, unlike the apply and
 * math functions, they return a fresh instance every time.
 * 
 */
public interface DoubleVector {

  /**
   * Retrieves the value at given index.
   * 
   * @param index the index.
   * @return a double value at the index.
   */
  public double get(int index);

  /**
   * Get the length of a vector, for sparse instance it is the actual length.
   * (not the dimension!) Always a constant time operation.
   * 
   * @return the length of the vector.
   */
  public int getLength();

  /**
   * Get the dimension of a vector, for dense instance it is the same like the
   * length, for sparse instances it is usually not the same. Always a constant
   * time operation.
   * 
   * @return the dimension of the vector.
   */
  public int getDimension();

  /**
   * Set a value at the given index.
   * 
   * @param index the index of the vector to set.
   * @param value the value at the index of the vector to set.
   */
  public void set(int index, double value);

  /**
   * Apply a given {@link DoubleVectorFunction} to this vector and return a new
   * one.
   * 
   * @param func the function to apply.
   * @return a new vector with the applied function.
   */
  public DoubleVector apply(DoubleVectorFunction func);

  /**
   * Apply a given {@link DoubleDoubleVectorFunction} to this vector and the
   * other given vector.
   * 
   * @param other the other vector.
   * @param func the function to apply on this and the other vector.
   * @return a new vector with the result of the function of the two vectors.
   */
  public DoubleVector apply(DoubleVector other, DoubleDoubleVectorFunction func);

  /**
   * Adds the given {@link DoubleVector} to this vector.
   * 
   * @param v the other vector.
   * @return a new vector with the sum of both vectors at each element index.
   */
  public DoubleVector add(DoubleVector v);

  /**
   * Adds the given scalar to this vector.
   * 
   * @param scalar the scalar.
   * @return a new vector with the result at each element index.
   */
  public DoubleVector add(double scalar);

  /**
   * Subtracts this vector by the given {@link DoubleVector}.
   * 
   * @param v the other vector.
   * @return a new vector with the difference of both vectors.
   */
  public DoubleVector subtract(DoubleVector v);

  /**
   * Subtracts the given scalar to this vector. (vector - scalar).
   * 
   * @param scalar the scalar.
   * @return a new vector with the result at each element index.
   */
  public DoubleVector subtract(double scalar);

  /**
   * Subtracts the given scalar from this vector. (scalar - vector).
   * 
   * @param scalar the scalar.
   * @return a new vector with the result at each element index.
   */
  public DoubleVector subtractFrom(double scalar);

  /**
   * Multiplies the given scalar to this vector.
   * 
   * @param scalar the scalar.
   * @return a new vector with the result of the operation.
   */
  public DoubleVector multiply(double scalar);

  /**
   * Multiplies the given {@link DoubleVector} with this vector.
   * 
   * @param vector the other vector.
   * @return a new vector with the result of the operation.
   */
  public DoubleVector multiply(DoubleVector vector);

  /**
   * Divides this vector by the given scalar. (= vector/scalar).
   * 
   * @param scalar the given scalar.
   * @return a new vector with the result of the operation.
   */
  public DoubleVector divide(double scalar);

  /**
   * Divides the given scalar by this vector. (= scalar/vector).
   * 
   * @param scalar the given scalar.
   * @return a new vector with the result of the operation.
   */
  public DoubleVector divideFrom(double scalar);

  /**
   * Powers this vector by the given amount. (=vector^x).
   * 
   * @param x the given exponent.
   * @return a new vector with the result of the operation.
   */
  public DoubleVector pow(int x);

  /**
   * Absolutes the vector at each element.
   * 
   * @return a new vector that does not contain negative values anymore.
   */
  public DoubleVector abs();

  /**
   * Square-roots each element.
   * 
   * @return a new vector.
   */
  public DoubleVector sqrt();

  /**
   * @return the sum of all elements in this vector.
   */
  public double sum();

  /**
   * Calculates the dot product between this vector and the given vector.
   * 
   * @param s the given vector s.
   * @return the dot product as a double.
   */
  public double dot(DoubleVector s);

  /**
   * Slices this vector from index 0 to the given length.
   * 
   * @param length must be > 0 and smaller than the dimension of the vector.
   * @return a new vector that is only length long.
   */
  public DoubleVector slice(int length);

  /**
   * Slices this vector from index offset with the given length. So you end at
   * the upper bound of (offset+length).
   * 
   * @param offset must be > 0 and smaller than the dimension of the vector
   * @param length must be > 0 and smaller than the dimension of the vector.
   *          This must be greater than the offset.
   * @return a new vector that is only (length) long.
   */
  public DoubleVector slice(int offset, int length);

  /**
   * @return the maximum element value in this vector.
   */
  public double max();

  /**
   * @return the minimum element value in this vector.
   */
  public double min();

  /**
   * @return an array representation of this vector.
   */
  public double[] toArray();

  /**
   * @return a fresh new copy of this vector, copies all elements to a new
   *         vector. (Does not reuse references or stuff).
   */
  public DoubleVector deepCopy();

  /**
   * @return an iterator that only iterates over non zero elements.
   */
  public Iterator<DoubleVectorElement> iterateNonZero();

  /**
   * @return an iterator that iterates over all elements.
   */
  public Iterator<DoubleVectorElement> iterate();

  /**
   * @return true if this instance is a sparse vector. Smarter and faster than
   *         instanceof.
   */
  public boolean isSparse();

  /**
   * @return true if this instance is a named vector.Smarter and faster than
   *         instanceof.
   */
  public boolean isNamed();

  /**
   * @return If this vector is a named instance, this will return its name. Or
   *         null if this is not a named instance.
   * 
   */
  public String getName();

  /**
   * Class for iteration of elements, consists of an index and a value at this
   * index. Can be reused for performance purposes.
   */
  public static final class DoubleVectorElement {

    private int index;
    private double value;

    public DoubleVectorElement() {
      super();
    }

    public DoubleVectorElement(int index, double value) {
      super();
      this.index = index;
      this.value = value;
    }

    public final int getIndex() {
      return index;
    }

    public final double getValue() {
      return value;
    }

    public final void setIndex(int in) {
      this.index = in;
    }

    public final void setValue(double in) {
      this.value = in;
    }
  }

}
