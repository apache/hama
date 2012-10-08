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
 * A {@link RegressionModel} for logistic regression
 */
public class LogisticRegressionModel implements RegressionModel {

  private final CostFunction costFunction;

  public LogisticRegressionModel() {
    costFunction = new CostFunction() {
      @Override
      public double calculateCostForItem(DoubleVector x, double y, DoubleVector theta, HypothesisFunction hypothesis) {
        return -1 * y * Math.log(applyHypothesis(theta, x)) + (1 - y) * Math.log(1 - applyHypothesis(theta, x));
      }
    };
  }

  @Override
  public double applyHypothesis(DoubleVector theta, DoubleVector x) {
    return 1d / (1d + Math.exp(-1 * theta.dot(x)));
  }

  @Override
  public double calculateCostForItem(DoubleVector x, double y, DoubleVector theta) {
    return costFunction.calculateCostForItem(x, y, theta, this);
  }
}
