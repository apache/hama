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

public class NamedFloatVector implements FloatVector {

  private final String name;
  private final FloatVector vector;

  public NamedFloatVector(String name, FloatVector deepCopy) {
    super();
    this.name = name;
    this.vector = deepCopy;
  }

  @Override
  public float get(int index) {
    return vector.get(index);
  }

  @Override
  public int getLength() {
    return vector.getLength();
  }

  @Override
  public int getDimension() {
    return vector.getDimension();
  }

  @Override
  public void set(int index, float value) {
    vector.set(index, value);
  }

  @Override
  public FloatVector applyToElements(FloatFunction func) {
    return vector.applyToElements(func);
  }

  @Override
  public FloatVector applyToElements(FloatVector other, FloatFloatFunction func) {
    return vector.applyToElements(other, func);
  }

  @Override
  public FloatVector addUnsafe(FloatVector vector2) {
    return vector.addUnsafe(vector2);
  }

  @Override
  public FloatVector add(FloatVector vector2) {
    return vector.add(vector2);
  }

  @Override
  public FloatVector add(float scalar) {
    return vector.add(scalar);
  }

  @Override
  public FloatVector subtractUnsafe(FloatVector vector2) {
    return vector.subtractUnsafe(vector2);
  }

  @Override
  public FloatVector subtract(FloatVector vector2) {
    return vector.subtract(vector2);
  }

  @Override
  public FloatVector subtract(float scalar) {
    return vector.subtract(scalar);
  }

  @Override
  public FloatVector subtractFrom(float scalar) {
    return vector.subtractFrom(scalar);
  }

  @Override
  public FloatVector multiply(float scalar) {
    return vector.multiply(scalar);
  }

  @Override
  public FloatVector multiplyUnsafe(FloatVector vector2) {
    return vector.multiplyUnsafe(vector2);
  }

  @Override
  public FloatVector multiply(FloatVector vector2) {
    return vector.multiply(vector2);
  }

  @Override
  public FloatVector multiply(FloatMatrix matrix) {
    return vector.multiply(matrix);
  }

  @Override
  public FloatVector multiplyUnsafe(FloatMatrix matrix) {
    return vector.multiplyUnsafe(matrix);
  }

  @Override
  public FloatVector divide(float scalar) {
    return vector.divide(scalar);
  }

  @Override
  public FloatVector divideFrom(float scalar) {
    return vector.divideFrom(scalar);
  }

  @Override
  public FloatVector pow(int x) {
    return vector.pow(x);
  }

  @Override
  public FloatVector abs() {
    return vector.abs();
  }

  @Override
  public FloatVector sqrt() {
    return vector.sqrt();
  }

  @Override
  public float sum() {
    return vector.sum();
  }

  @Override
  public float dotUnsafe(FloatVector vector2) {
    return vector.dotUnsafe(vector2);
  }

  @Override
  public float dot(FloatVector vector2) {
    return vector.dot(vector2);
  }

  @Override
  public FloatVector slice(int length) {
    return vector.slice(length);
  }

  @Override
  public FloatVector sliceUnsafe(int length) {
    return vector.sliceUnsafe(length);
  }

  @Override
  public FloatVector slice(int start, int end) {
    return vector.slice(start, end);
  }

  @Override
  public FloatVector sliceUnsafe(int start, int end) {
    return vector.sliceUnsafe(start, end);
  }

  @Override
  public float max() {
    return vector.max();
  }

  @Override
  public float min() {
    return vector.min();
  }

  @Override
  public float[] toArray() {
    return vector.toArray();
  }

  @Override
  public FloatVector deepCopy() {
    return new NamedFloatVector(name, vector.deepCopy());
  }

  @Override
  public Iterator<FloatVectorElement> iterateNonDefault() {
    return vector.iterateNonDefault();
  }

  @Override
  public Iterator<FloatVectorElement> iterate() {
    return vector.iterate();
  }

  @Override
  public boolean isSparse() {
    return vector.isSparse();
  }

  @Override
  public boolean isNamed() {
    return true;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return String.format("%s: %s", name, vector.toString());
  }

}
