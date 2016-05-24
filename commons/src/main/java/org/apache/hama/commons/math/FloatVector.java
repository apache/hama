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

import java.util.Iterator;

public interface FloatVector {


  /**
   * Retrieves the value at given index.
   * 
   * @param index the index.
   * @return a float value at the index.
   */
  public float get(int index);

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
  public void set(int index, float value);

  /**
   * Apply a given {@link FloatVectorFunction} to this vector and return a new
   * one.
   * 
   * @param func the function to apply.
   * @return a new vector with the applied function.
   */
  public FloatVector applyToElements(FloatFunction func);

  /**
   * Apply a given {@link DoubleFloatVectorFunction} to this vector and the
   * other given vector.
   * 
   * @param other the other vector.
   * @param func the function to apply on this and the other vector.
   * @return a new vector with the result of the function of the two vectors.
   */
  public FloatVector applyToElements(FloatVector other,
      FloatFloatFunction func);

  /**
   * Adds the given {@link FloatVector} to this vector.
   * 
   * @param vector the other vector.
   * @return a new vector with the sum of both vectors at each element index.
   */
  public FloatVector addUnsafe(FloatVector vector);

  /**
   * Validates the input and adds the given {@link FloatVector} to this vector.
   * 
   * @param vector the other vector.
   * @return a new vector with the sum of both vectors at each element index.
   */
  public FloatVector add(FloatVector vector);

  /**
   * Adds the given scalar to this vector.
   * 
   * @param scalar the scalar.
   * @return a new vector with the result at each element index.
   */
  public FloatVector add(float scalar);

  /**
   * Subtracts this vector by the given {@link FloatVector}.
   * 
   * @param vector the other vector.
   * @return a new vector with the difference of both vectors.
   */
  public FloatVector subtractUnsafe(FloatVector vector);

  /**
   * Validates the input and subtracts this vector by the given
   * {@link FloatVector}.
   * 
   * @param vector the other vector.
   * @return a new vector with the difference of both vectors.
   */
  public FloatVector subtract(FloatVector vector);

  /**
   * Subtracts the given scalar to this vector. (vector - scalar).
   * 
   * @param scalar the scalar.
   * @return a new vector with the result at each element index.
   */
  public FloatVector subtract(float scalar);

  /**
   * Subtracts the given scalar from this vector. (scalar - vector).
   * 
   * @param scalar the scalar.
   * @return a new vector with the result at each element index.
   */
  public FloatVector subtractFrom(float scalar);

  /**
   * Multiplies the given scalar to this vector.
   * 
   * @param scalar the scalar.
   * @return a new vector with the result of the operation.
   */
  public FloatVector multiply(float scalar);

  /**
   * Multiplies the given {@link FloatVector} with this vector.
   * 
   * @param vector the other vector.
   * @return a new vector with the result of the operation.
   */
  public FloatVector multiplyUnsafe(FloatVector vector);

  /**
   * Validates the input and multiplies the given {@link FloatVector} with this
   * vector.
   * 
   * @param vector the other vector.
   * @return a new vector with the result of the operation.
   */
  public FloatVector multiply(FloatVector vector);

  /**
   * Validates the input and multiplies the given {@link FloatMatrix} with this
   * vector.
   * 
   * @param matrix
   * @return a new vector with the result of the operation.
   */
  public FloatVector multiply(FloatMatrix matrix);

  /**
   * Multiplies the given {@link FloatMatrix} with this vector.
   * 
   * @param matrix
   * @return a new vector with the result of the operation.
   */
  public FloatVector multiplyUnsafe(FloatMatrix matrix);

  /**
   * Divides this vector by the given scalar. (= vector/scalar).
   * 
   * @param scalar the given scalar.
   * @return a new vector with the result of the operation.
   */
  public FloatVector divide(float scalar);

  /**
   * Divides the given scalar by this vector. (= scalar/vector).
   * 
   * @param scalar the given scalar.
   * @return a new vector with the result of the operation.
   */
  public FloatVector divideFrom(float scalar);

  /**
   * Powers this vector by the given amount. (=vector^x).
   * 
   * @param x the given exponent.
   * @return a new vector with the result of the operation.
   */
  public FloatVector pow(int x);

  /**
   * Absolutes the vector at each element.
   * 
   * @return a new vector that does not contain negative values anymore.
   */
  public FloatVector abs();

  /**
   * Square-roots each element.
   * 
   * @return a new vector.
   */
  public FloatVector sqrt();

  /**
   * @return the sum of all elements in this vector.
   */
  public float sum();

  /**
   * Calculates the dot product between this vector and the given vector.
   * 
   * @param vector the given vector.
   * @return the dot product as a float.
   */
  public float dotUnsafe(FloatVector vector);

  /**
   * Validates the input and calculates the dot product between this vector and
   * the given vector.
   * 
   * @param vector the given vector.
   * @return the dot product as a float.
   */
  public float dot(FloatVector vector);

  /**
   * Validates the input and slices this vector from index 0 to the given
   * length.
   * 
   * @param length must be > 0 and smaller than the dimension of the vector.
   * @return a new vector that is only length long.
   */
  public FloatVector slice(int length);

  /**
   * Slices this vector from index 0 to the given length.
   * 
   * @param length must be > 0 and smaller than the dimension of the vector.
   * @return a new vector that is only length long.
   */
  public FloatVector sliceUnsafe(int length);

  /**
   * Validates the input and then slices this vector from start to end, both are
   * INCLUSIVE. For example vec = [0, 1, 2, 3, 4, 5], vec.slice(2, 5) = [2, 3,
   * 4, 5].
   * 
   * @param start must be >= 0 and smaller than the dimension of the vector
   * @param end must be >= 0 and smaller than the dimension of the vector.
   *          This must be greater than or equal to the start.
   * @return a new vector that is only (length) long.
   */
  public FloatVector slice(int start, int end);

  /**
   * Slices this vector from start to end, both are INCLUSIVE. For example vec =
   * [0, 1, 2, 3, 4, 5], vec.slice(2, 5) = [2, 3, 4, 5].
   * 
   * @param start must be >= 0 and smaller than the dimension of the vector
   * @param end must be >= 0 and smaller than the dimension of the vector.
   *          This must be greater than or equal to the start.
   * @return a new vector that is only (length) long.
   */
  public FloatVector sliceUnsafe(int start, int end);

  /**
   * @return the maximum element value in this vector.
   */
  public float max();

  /**
   * @return the minimum element value in this vector.
   */
  public float min();

  /**
   * @return an array representation of this vector.
   */
  public float[] toArray();

  /**
   * @return a fresh new copy of this vector, copies all elements to a new
   *         vector. (Does not reuse references or stuff).
   */
  public FloatVector deepCopy();

  /**
   * @return an iterator that only iterates over non default elements.
   */
  public Iterator<FloatVectorElement> iterateNonDefault();

  /**
   * @return an iterator that iterates over all elements.
   */
  public Iterator<FloatVectorElement> iterate();

  /**
   * Return whether the vector is a sparse vector.
   * @return true if this instance is a sparse vector. Smarter and faster than
   *         instanceof.
   */
  public boolean isSparse();

  /**
   * Return whether the vector is a named vector.
   * @return true if this instance is a named vector.Smarter and faster than
   *         instanceof.
   */
  public boolean isNamed();

  /**
   * Get the name of the vector. 
   * 
   * @return If this vector is a named instance, this will return its name. Or
   *         null if this is not a named instance.
   * 
   */
  public String getName();

  /**
   * Class for iteration of elements, consists of an index and a value at this
   * index. Can be reused for performance purposes.
   */
  public static final class FloatVectorElement {

    private int index;
    private float value;

    public FloatVectorElement() {
      super();
    }

    public FloatVectorElement(int index, float value) {
      super();
      this.index = index;
      this.value = value;
    }

    public final int getIndex() {
      return index;
    }

    public final float getValue() {
      return value;
    }

    public final void setIndex(int in) {
      this.index = in;
    }

    public final void setValue(float in) {
      this.value = in;
    }
  }
}
