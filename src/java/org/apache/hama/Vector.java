/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama;

import java.util.Iterator;

import org.apache.hadoop.hbase.io.Cell;

/**
 * Basic vector interface.
 */
public interface Vector {

  /**
   * Size of the vector
   * 
   * @return size of the vector
   */
  public int size();

  /**
   * Gets the value of index
   * 
   * @param index
   * @return v(index)
   */
  public double get(int index);

  /**
   * Sets the value of index
   * 
   * @param index
   * @param value
   */
  public void set(int index, double value);

  /**
   * Sets the vector
   * 
   * @param v
   * @return x = v
   */
  public Vector set(Vector v);

  /**
   * Adds the value to v(index)
   * 
   * @param index
   * @param value
   */
  public void add(int index, double value);

  /**
   * x = alpha*v + x
   * 
   * @param alpha
   * @param v
   * @return x = alpha*v + x
   */
  public Vector add(double alpha, Vector v);

  /**
   * x = v + x
   * 
   * @param v
   * @return x = v + x
   */
  public Vector add(Vector v);

  /**
   * x dot v
   * 
   * @param v
   * @return x dot v
   */
  public double dot(Vector v);

  /**
   * v = alpha*v 
   * 
   * @param alpha
   * @return v = alpha*v
   */
  public Vector scale(double alpha);
  
  
  /**
   * Computes the given norm of the vector
   * 
   * @param type
   * @return norm of the vector
   */
  public double norm(Norm type);

  /**
   * Supported vector-norms.
   */
  enum Norm {

    /** Sum of the absolute values of the entries */
    One,

    /** The root of sum of squares */
    Two,

    /**
     * As the 2 norm may overflow, an overflow resistant version is also
     * available. Note that it may be slower.
     */
    TwoRobust,

    /** Largest entry in absolute value */
    Infinity
  }

  /**
   * Returns an iterator
   * 
   * @return iterator
   */
  public Iterator<Cell> iterator();
}
