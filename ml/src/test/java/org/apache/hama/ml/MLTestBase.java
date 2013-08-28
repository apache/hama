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
package org.apache.hama.ml;

import java.util.Arrays;
import java.util.List;

/**
 * The common methods for testing machine learning algorithms 
 *
 */
public abstract class MLTestBase {

  /**
   * Conduct the 0-1 normalization.
   * @param instances
   */
  protected static void zeroOneNormalization(List<double[]> instanceList, int len) {
    int dimension = len;

    double[] mins = new double[dimension];
    double[] maxs = new double[dimension];
    Arrays.fill(mins, Double.MAX_VALUE);
    Arrays.fill(maxs, Double.MIN_VALUE);

    for (double[] instance : instanceList) {
      for (int i = 0; i < len; ++i) {
        if (mins[i] > instance[i]) {
          mins[i] = instance[i];
        }
        if (maxs[i] < instance[i]) {
          maxs[i] = instance[i];
        }
      }
    }
    
    for (double[] instance : instanceList) {
      for (int i = 0; i < len; ++i) {
        double range = maxs[i] - mins[i];
        if (range != 0) {
          instance[i] = (instance[i] - mins[i]) / range;
        }
      }
    }
    
  }
}
