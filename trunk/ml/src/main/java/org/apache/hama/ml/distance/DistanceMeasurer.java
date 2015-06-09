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
package org.apache.hama.ml.distance;

import org.apache.hama.commons.math.DoubleVector;

/**
 * a {@link DistanceMeasurer} is responsible for calculating the distance
 * between {@link DoubleVector}s or Arrays of {@code double}s
 */
public interface DistanceMeasurer {

  /**
   * Calculates the distance between two arrays of {@code double}s
   * 
   * @param set1 an array of {@code double}
   * @param set2 an array of {@code double}
   * @return a {@code double} representing the distance
   */
  public double measureDistance(double[] set1, double[] set2);

  /**
   * Calculates the distance between two {@link DoubleVector}ss
   * 
   * @param vec1 a {@link DoubleVector}
   * @param vec2 a {@link DoubleVector}
   * @return a {@code double} representing the distance
   */
  public double measureDistance(DoubleVector vec1, DoubleVector vec2);

}
