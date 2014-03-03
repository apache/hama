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

public final class NamedDoubleVector implements DoubleVector {
  
  private final String name;
  private final DoubleVector vector;
  
  public NamedDoubleVector(String name, DoubleVector deepCopy) {
    super();
    this.name = name;
    this.vector = deepCopy;
  }

  @Override
  public double get(int index) {
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
  public void set(int index, double value) {
    vector.set(index, value);
  }

  @Override
  public DoubleVector applyToElements(DoubleFunction func) {
    return vector.applyToElements(func);
  }

  @Override
  public DoubleVector applyToElements(DoubleVector other,
      DoubleDoubleFunction func) {
    return vector.applyToElements(other, func);
  }

  @Override
  public DoubleVector addUnsafe(DoubleVector vector2) {
    return vector.addUnsafe(vector2);
  }

  @Override
  public DoubleVector add(DoubleVector vector2) {
    return vector.add(vector2);
  }

  @Override
  public DoubleVector add(double scalar) {
    return vector.add(scalar);
  }

  @Override
  public DoubleVector subtractUnsafe(DoubleVector vector2) {
    return vector.subtractUnsafe(vector2);
  }

  @Override
  public DoubleVector subtract(DoubleVector vector2) {
    return vector.subtract(vector2);
  }

  @Override
  public DoubleVector subtract(double scalar) {
    return vector.subtract(scalar);
  }

  @Override
  public DoubleVector subtractFrom(double scalar) {
    return vector.subtractFrom(scalar);
  }

  @Override
  public DoubleVector multiply(double scalar) {
    return vector.multiply(scalar);
  }

  @Override
  public DoubleVector multiplyUnsafe(DoubleVector vector2) {
    return vector.multiplyUnsafe(vector2);
  }

  @Override
  public DoubleVector multiply(DoubleVector vector2) {
    return vector.multiply(vector2);
  }

  @Override
  public DoubleVector multiply(DoubleMatrix matrix) {
    return vector.multiply(matrix);
  }

  @Override
  public DoubleVector multiplyUnsafe(DoubleMatrix matrix) {
    return vector.multiplyUnsafe(matrix);
  }

  @Override
  public DoubleVector divide(double scalar) {
    return vector.divide(scalar);
  }

  @Override
  public DoubleVector divideFrom(double scalar) {
    return vector.divideFrom(scalar);
  }

  @Override
  public DoubleVector pow(int x) {
    return vector.pow(x);
  }

  @Override
  public DoubleVector abs() {
    return vector.abs();
  }

  @Override
  public DoubleVector sqrt() {
    return vector.sqrt();
  }

  @Override
  public double sum() {
    return vector.sum();
  }

  @Override
  public double dotUnsafe(DoubleVector vector2) {
    return vector.dotUnsafe(vector2);
  }

  @Override
  public double dot(DoubleVector vector2) {
    return vector.dot(vector2);
  }

  @Override
  public DoubleVector slice(int length) {
    return vector.slice(length);
  }

  @Override
  public DoubleVector sliceUnsafe(int length) {
    return vector.sliceUnsafe(length);
  }

  @Override
  public DoubleVector slice(int start, int end) {
    return vector.slice(start, end);
  }

  @Override
  public DoubleVector sliceUnsafe(int start, int end) {
    return vector.sliceUnsafe(start, end);
  }

  @Override
  public double max() {
    return vector.max();
  }

  @Override
  public double min() {
    return vector.min();
  }

  @Override
  public double[] toArray() {
    return vector.toArray();
  }

  @Override
  public DoubleVector deepCopy() {
    return new NamedDoubleVector(name, vector.deepCopy());
  }

  @Override
  public Iterator<DoubleVectorElement> iterateNonZero() {
    return vector.iterateNonZero();
  }

  @Override
  public Iterator<DoubleVectorElement> iterate() {
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
