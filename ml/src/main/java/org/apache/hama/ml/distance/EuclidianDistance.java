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

import org.apache.hama.ml.math.DoubleVector;

public final class EuclidianDistance implements DistanceMeasurer {

  @Override
  public double measureDistance(double[] set1, double[] set2) {
    double sum = 0;
    int length = set1.length;
    for (int i = 0; i < length; i++) {
      double diff = set2[i] - set1[i];
      // multiplication is faster than Math.pow() for ^2.
      sum += (diff * diff);
    }

    return Math.sqrt(sum);
  }

  @Override
  public double measureDistance(DoubleVector vec1, DoubleVector vec2) {
    return Math.sqrt(vec2.subtract(vec1).pow(2).sum());
  }

}
