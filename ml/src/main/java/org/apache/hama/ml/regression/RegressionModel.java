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
package org.apache.hama.ml.regression;

import org.apache.hama.ml.math.DoubleVector;

/**
 * A cost model for gradient descent based regression
 */
public interface RegressionModel extends HypothesisFunction {

  /**
   * Calculates the cost function for a given item (input x, output y) and the
   * model's parameters defined by the vector theta
   * 
   * @param x the input vector
   * @param y the learned output for x
   * @param m the total number of existing items
   * @param theta the parameters vector theta
   * @return the calculated cost for input x and output y
   */
  public double calculateCostForItem(DoubleVector x, double y, int m,
      DoubleVector theta);

}
