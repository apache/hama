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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/**
 * The implementation of SparseVector.
 * 
 */
public class SparseDoubleVector implements DoubleVector {

  private int dimension;
  private double defaultValue; // 0 by default
  private Map<Integer, Double> elements; // the non-default value

  /**
   * Initialize a sparse vector with given dimension with default value 0.
   * 
   * @param dimension
   */
  public SparseDoubleVector(int dimension) {
    this(dimension, 0.0);
  }

  /**
   * Initialize a sparse vector with given dimension and given default value 0.
   * 
   * @param dimension
   * @param defaultValue
   */
  public SparseDoubleVector(int dimension, double defaultValue) {
    this.elements = new HashMap<Integer, Double>();
    this.defaultValue = defaultValue;
    this.dimension = dimension;
  }

  /**
   * Get the value of a given index.
   */
  @Override
  public double get(int index) {
    Preconditions.checkArgument(index < this.dimension,
        "Index out of max allowd dimension of sparse vector.");
    Double val = this.elements.get(index);
    if (val == null) {
      val = this.defaultValue;
    }
    return val;
  }

  /**
   * Get the dimension of the vector.
   */
  @Override
  public int getLength() {
    return this.getDimension();
  }

  /**
   * Get the dimension of the vector.
   */
  @Override
  public int getDimension() {
    return this.dimension;
  }

  /**
   * Set the value of a given index.
   */
  @Override
  public void set(int index, double value) {
    Preconditions.checkArgument(index < this.dimension, String.format(
        "Index %d out of max allowd dimension %d of sparse vector.", index,
        this.dimension));
    if (value == this.defaultValue) {
      this.elements.remove(index);
    } else {
      this.elements.put(index, value);
    }
  }

  /**
   * Apply a function to the copy of current vector and return the result.
   */
  @Override
  public DoubleVector applyToElements(DoubleFunction func) {
    SparseDoubleVector newVec = new SparseDoubleVector(this.dimension,
        func.apply(this.defaultValue));
    // apply function to all non-empty entries
    for (Map.Entry<Integer, Double> entry : this.elements.entrySet()) {
      newVec.elements.put(entry.getKey(), func.apply(entry.getValue()));
    }

    return newVec;
  }

  /**
   * Apply a binary function to the copy of current vector with another vector
   * and then return the result.
   */
  @Override
  public DoubleVector applyToElements(DoubleVector other,
      DoubleDoubleFunction func) {

    Preconditions.checkArgument(this.getDimension() == other.getDimension(),
        "Dimension of two vectors should be equal.");

    double otherDefaultValue = 0.0;
    if (other instanceof SparseDoubleVector) {
      otherDefaultValue = ((SparseDoubleVector) other).defaultValue;
    }
    double newDefaultValue = this.defaultValue;
    if (other instanceof SparseDoubleVector) {
      newDefaultValue = func.apply(this.defaultValue, otherDefaultValue);
    }

    SparseDoubleVector newVec = new SparseDoubleVector(this.dimension,
        newDefaultValue);

    Iterator<DoubleVectorElement> thisItr = this.iterateNonDefault();
    Iterator<DoubleVectorElement> otherItr = other.iterateNonDefault();

    DoubleVectorElement thisCur = null;
    if (thisItr.hasNext()) {
      thisCur = thisItr.next();
    }
    DoubleVectorElement otherCur = null;
    if (otherItr.hasNext()) {
      otherCur = otherItr.next();
    }

    while (thisCur != null || otherCur != null) {
      if (thisCur == null) { // the iterator of current vector reaches the end
        newVec.set(otherCur.getIndex(),
            func.apply(this.defaultValue, otherCur.getValue()));
        if (newVec.get(otherCur.getIndex()) == newVec.defaultValue) {
          // remove if the value equals the default value
          newVec.elements.remove(otherCur.getIndex());
        }
        if (otherItr.hasNext()) {
          otherCur = otherItr.next();
        } else {
          otherCur = null;
        }
      } else if (otherCur == null) {
        newVec.set(thisCur.getIndex(),
            func.apply(thisCur.getValue(), otherDefaultValue));
        if (newVec.get(thisCur.getIndex()) == newVec.defaultValue) {
          // remove if the value equals the default value
          newVec.elements.remove(thisCur.getIndex());
        }
        if (thisItr.hasNext()) {
          thisCur = thisItr.next();
        } else {
          thisCur = null;
        }
      } else {
        int curIdx = 0;
        if (thisCur.getIndex() < otherCur.getIndex()) {
          curIdx = thisCur.getIndex();
          newVec.set(curIdx, func.apply(thisCur.getValue(), otherDefaultValue));
          if (thisItr.hasNext()) {
            thisCur = thisItr.next();
          } else {
            thisCur = null;
          }
        } else if (thisCur.getIndex() > otherCur.getIndex()) {
          curIdx = otherCur.getIndex();
          newVec
              .set(curIdx, func.apply(this.defaultValue, otherCur.getValue()));
          if (otherItr.hasNext()) {
            otherCur = otherItr.next();
          } else {
            otherCur = null;
          }
        } else {
          curIdx = thisCur.getIndex();
          newVec.set(curIdx,
              func.apply(thisCur.getValue(), otherCur.getValue()));
          if (thisItr.hasNext()) {
            thisCur = thisItr.next();
          } else {
            thisCur = null;
          }
          if (otherItr.hasNext()) {
            otherCur = otherItr.next();
          } else {
            otherCur = null;
          }
        }
        if (newVec.get(curIdx) == newVec.defaultValue) {
          // remove if the value equals the default value
          newVec.elements.remove(curIdx);
        }
      }
    }

    return newVec;
  }

  /**
   * Add another vector.
   */
  @Override
  public DoubleVector addUnsafe(DoubleVector vector) {
    return this.applyToElements(vector, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return x1 + x2;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Add another vector to the copy of the current vector and return the result.
   */
  @Override
  public DoubleVector add(DoubleVector vector) {
    Preconditions.checkArgument(this.dimension == vector.getDimension(),
        "Dimensions of two vectors are not the same.");
    return this.addUnsafe(vector);
  }

  /**
   * Add a scalar.
   */
  @Override
  public DoubleVector add(double scalar) {
    final double val = scalar;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value + val;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Subtract a vector from current vector.
   */
  @Override
  public DoubleVector subtractUnsafe(DoubleVector vector) {
    return this.applyToElements(vector, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return x1 - x2;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        return 0;
      }
    });
  }

  /**
   * Subtract a vector from current vector.
   */
  @Override
  public DoubleVector subtract(DoubleVector vector) {
    Preconditions.checkArgument(this.dimension == vector.getDimension(),
        "Dimensions of two vector are not the same.");
    return this.subtractUnsafe(vector);
  }

  /**
   * Subtract a scalar from a copy of the current vector.
   */
  @Override
  public DoubleVector subtract(double scalar) {
    final double val = scalar;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value - val;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Subtract a scalar from a copy of the current vector and return the result.
   */
  @Override
  public DoubleVector subtractFrom(double scalar) {
    final double val = scalar;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return val - value;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Multiply a copy of the current vector by a scalar and return the result.
   */
  @Override
  public DoubleVector multiply(double scalar) {
    final double val = scalar;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return val * value;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Multiply a copy of the current vector with another vector and return the
   * result.
   */
  @Override
  public DoubleVector multiplyUnsafe(DoubleVector vector) {
    return this.applyToElements(vector, new DoubleDoubleFunction() {
      @Override
      public double apply(double x1, double x2) {
        return x1 * x2;
      }

      @Override
      public double applyDerivative(double x1, double x2) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Multiply a copy of the current vector with another vector and return the
   * result.
   */
  @Override
  public DoubleVector multiply(DoubleVector vector) {
    Preconditions.checkArgument(this.dimension == vector.getDimension(),
        "Dimensions of two vectors are not the same.");
    return this.multiplyUnsafe(vector);
  }

  /**
   * Multiply a copy of the current vector with a matrix and return the result,
   * i.e. r = v * M.
   */
  @Override
  public DoubleVector multiply(DoubleMatrix matrix) {
    Preconditions
        .checkArgument(this.dimension == matrix.getColumnCount(),
            "The dimension of vector does not equal to the dimension of the matrix column.");
    return this.multiplyUnsafe(matrix);
  }

  /**
   * Multiply a copy of the current vector with a matrix and return the result,
   * i.e. r = v * M.
   */
  @Override
  public DoubleVector multiplyUnsafe(DoubleMatrix matrix) {
    // currently the result is a dense double vector
    DoubleVector res = new DenseDoubleVector(matrix.getColumnCount());
    int columns = matrix.getColumnCount();
    for (int i = 0; i < columns; ++i) {
      res.set(i, this.dotUnsafe(matrix.getColumnVector(i)));
    }
    return res;
  }

  /**
   * Divide a copy of the current vector by a scala.
   */
  @Override
  public DoubleVector divide(double scalar) {
    Preconditions.checkArgument(scalar != 0, "Scalar cannot be 0.");
    final double factor = scalar;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return value / factor;
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#divideFrom(double)
   */
  @Override
  public DoubleVector divideFrom(double scalar) {
    Preconditions.checkArgument(scalar != 0, "Scalar cannot be 0.");
    final double factor = scalar;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return factor / value; // value can be 0!
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Apply the power to each element of a copy of the current vector.
   */
  @Override
  public DoubleVector pow(int x) {
    final int p = x;
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return Math.pow(value, p);
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Apply the abs elementwise to a copy of current vector.
   */
  @Override
  public DoubleVector abs() {
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return Math.abs(value);
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Apply the sqrt elementwise to a copy of current vector.
   */
  @Override
  public DoubleVector sqrt() {
    return this.applyToElements(new DoubleFunction() {
      @Override
      public double apply(double value) {
        return Math.sqrt(value);
      }

      @Override
      public double applyDerivative(double value) {
        throw new UnsupportedOperationException();
      }
    });
  }

  /**
   * Get the sum of all the elements.
   */
  @Override
  public double sum() {
    double sum = 0.0;
    Iterator<DoubleVectorElement> itr = this.iterate();
    while (itr.hasNext()) {
      sum += itr.next().getValue();
    }
    return sum;
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hama.commons.math.DoubleVector#dotUnsafe(org.apache.hama.commons
   * .math.DoubleVector)
   */
  @Override
  public double dotUnsafe(DoubleVector vector) {
    return this.multiplyUnsafe(vector).sum();
  }

  /**
   * Apply dot onto a copy of current vector and another vector.
   */
  @Override
  public double dot(DoubleVector vector) {
    Preconditions.checkArgument(this.dimension == vector.getDimension(),
        "Dimensions of two vectors are not equal.");
    return this.dotUnsafe(vector);
  }

  /**
   * Obtain a sub-vector from the current vector.
   */
  @Override
  public DoubleVector slice(int length) {
    Preconditions.checkArgument(length >= 0 && length < this.dimension,
        String.format("length must be in range [0, %d).", this.dimension));
    return this.sliceUnsafe(length);
  }

  /**
   * Obtain a sub-vector from the current vector.
   */
  @Override
  public DoubleVector sliceUnsafe(int length) {
    DoubleVector newVector = new SparseDoubleVector(length, this.defaultValue);
    for (Map.Entry<Integer, Double> entry : this.elements.entrySet()) {
      if (entry.getKey() >= length) {
        continue;
      }
      newVector.set(entry.getKey(), entry.getValue());
    }
    return newVector;
  }

  /**
   * Obtain a sub-vector of the current vector
   * 
   * @param start inclusive
   * @param end inclusive
   */
  @Override
  public DoubleVector slice(int start, int end) {
    Preconditions.checkArgument(start >= 0 && end < this.dimension, String
        .format("Start and end range should be in [0, %d].", this.dimension));
    return this.sliceUnsafe(start, end);
  }

  /**
   * @param start inclusive
   * @param end inclusive
   */
  @Override
  public DoubleVector sliceUnsafe(int start, int end) {
    SparseDoubleVector slicedVec = new SparseDoubleVector(end - start + 1,
        this.defaultValue);
    for (Map.Entry<Integer, Double> entry : this.elements.entrySet()) {
      if (entry.getKey() >= start && entry.getKey() <= end) {
        slicedVec.elements.put(entry.getKey() - start, entry.getValue());
      }
      if (entry.getKey() > end) {
        continue;
      }
    }
    return slicedVec;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#max()
   */
  @Override
  public double max() {
    double max = this.defaultValue;
    for (Map.Entry<Integer, Double> entry : this.elements.entrySet()) {
      max = Math.max(max, entry.getValue());
    }
    return max;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#min()
   */
  @Override
  public double min() {
    double min = this.defaultValue;
    for (Map.Entry<Integer, Double> entry : this.elements.entrySet()) {
      min = Math.min(min, entry.getValue());
    }
    return min;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#toArray()
   */
  @Override
  public double[] toArray() {
    throw new UnsupportedOperationException(
        "SparseDoubleVector does not support toArray() method.");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#deepCopy()
   */
  @Override
  public DoubleVector deepCopy() {
    SparseDoubleVector copy = new SparseDoubleVector(this.dimension);
    copy.elements = new HashMap<Integer, Double>(this.elements.size());
    copy.elements.putAll(this.elements);
    return copy;
  }

  /**
   * Generate the iterator that iterates the non-default values.
   */
  @Override
  public Iterator<DoubleVectorElement> iterateNonDefault() {
    return new NonDefaultIterator();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#iterate()
   */
  @Override
  public Iterator<DoubleVectorElement> iterate() {
    return new DefaultIterator();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#isSparse()
   */
  @Override
  public boolean isSparse() {
    return true;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#isNamed()
   */
  @Override
  public boolean isNamed() {
    return false;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hama.commons.math.DoubleVector#getName()
   */
  @Override
  public String getName() {
    return null;
  }

  /**
   * Non-zero iterator for vector elements.
   */
  private final class NonDefaultIterator extends
      AbstractIterator<DoubleVectorElement> {
    private final DoubleVectorElement element = new DoubleVectorElement();

    private final int entryDimension;
    private final Map<Integer, Double> entries;
    private final double defaultV = defaultValue;
    private int currentIndex = 0;

    public NonDefaultIterator() {
      this.entryDimension = dimension;
      this.entries = elements;
    }

    @Override
    protected DoubleVectorElement computeNext() {
      DoubleVectorElement elem = getNext();
      // skip the entries with default values
      while (elem != null && elem.getValue() == this.defaultV) {
        elem = getNext();
      }
      return elem;
    }

    private DoubleVectorElement getNext() {
      if (currentIndex < entryDimension) {
        Double value = entries.get(currentIndex);
        element.setIndex(currentIndex);
        if (value == null) {
          element.setValue(defaultV);
        } else {
          element.setValue(value);
        }
        ++currentIndex;
        return element;
      } else {
        return endOfData();
      }
    }
  }

  private final class DefaultIterator extends
      AbstractIterator<DoubleVectorElement> {

    private final DoubleVectorElement element = new DoubleVectorElement();

    private final int entryDimension;
    private final Map<Integer, Double> entries;
    private final double defaultV = defaultValue;
    private int currentIndex = 0;

    public DefaultIterator() {
      this.entryDimension = dimension;
      this.entries = elements;
    }

    @Override
    protected DoubleVectorElement computeNext() {
      if (currentIndex < entryDimension) {
        Double value = entries.get(currentIndex);
        element.setIndex(currentIndex);
        if (value == null) {
          element.setValue(defaultV);
        } else {
          element.setValue(value);
        }
        ++currentIndex;
        return element;
      } else {
        return endOfData();
      }
    }

  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator<DoubleVectorElement> itr = this.iterate();
    sb.append('{');
    while (itr.hasNext()) {
      DoubleVectorElement elem = itr.next();
      sb.append(String.format("%s: %f, ", elem.getIndex(), elem.getValue()));
    }
    sb.replace(sb.length() - 2, sb.length() - 1, "}");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(defaultValue);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + dimension;
    result = prime * result + ((elements == null) ? 0 : elements.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DoubleVector) {
      DoubleVector otherVec = (DoubleVector) other;
      if (this.dimension != otherVec.getDimension()) {
        return false;
      }
      // check non-default entries first
      for (Map.Entry<Integer, Double> entry : this.elements.entrySet()) {
        if (Math.abs(entry.getValue() - otherVec.get(entry.getKey())) > 0.000001) {
          return false;
        }
      }
      // check default values
      for (int i = 0; i < this.dimension; ++i) {
        if (Math.abs(this.get(i) - otherVec.get(i)) > 0.000001) {
          return false;
        }
      }

      return true;
    }
    return false;
  }
}
