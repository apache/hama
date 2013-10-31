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

public final class CosineDistance implements DistanceMeasurer {

  @Override
  public double measureDistance(double[] set1, double[] set2) {
    double dotProduct = 0.0;
    double lengthSquaredp1 = 0.0;
    double lengthSquaredp2 = 0.0;
    for (int i = 0; i < set1.length; i++) {
      lengthSquaredp1 += set1[i] * set1[i];
      lengthSquaredp2 += set2[i] * set2[i];
      dotProduct += set1[i] * set2[i];
    }
    double denominator = Math.sqrt(lengthSquaredp1)
        * Math.sqrt(lengthSquaredp2);

    // correct for floating-point rounding errors
    if (denominator < dotProduct) {
      denominator = dotProduct;
    }
    // prevent NaNs
    if (denominator == 0.0d)
      return 1.0;

    return 1.0 - dotProduct / denominator;
  }

  @Override
  public double measureDistance(DoubleVector vec1, DoubleVector vec2) {
    double lengthSquaredv1 = vec1.pow(2).sum();
    double lengthSquaredv2 = vec2.pow(2).sum();

    double dotProduct = vec2.dotUnsafe(vec1);
    double denominator = Math.sqrt(lengthSquaredv1)
        * Math.sqrt(lengthSquaredv2);

    // correct for floating-point rounding errors
    if (denominator < dotProduct) {
      denominator = dotProduct;
    }

    return 1.0 - dotProduct / denominator;
  }

}
