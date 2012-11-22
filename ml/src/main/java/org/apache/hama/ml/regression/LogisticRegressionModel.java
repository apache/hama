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

import java.math.BigDecimal;
import java.math.MathContext;

import org.apache.hama.ml.math.DoubleVector;

/**
 * A {@link RegressionModel} for logistic regression
 */
public class LogisticRegressionModel implements RegressionModel {

  private final CostFunction costFunction;

  public LogisticRegressionModel() {
    costFunction = new CostFunction() {
      @Override
      public double calculateCostForItem(DoubleVector x, double y, int m,
          DoubleVector theta, HypothesisFunction hypothesis) {
        return (-1d * y
            * ln(applyHypothesisWithPrecision(theta, x)).doubleValue() + (1d - y)
            * ln(
                applyHypothesisWithPrecision(theta, x).subtract(
                    BigDecimal.valueOf(1))).doubleValue())
            / m;
      }
    };
  }

  @Override
  public double applyHypothesis(DoubleVector theta, DoubleVector x) {
    return applyHypothesisWithPrecision(theta, x).doubleValue();
  }

  private BigDecimal applyHypothesisWithPrecision(DoubleVector theta,
      DoubleVector x) {
    return BigDecimal.valueOf(1).divide(
        BigDecimal.valueOf(1d).add(
            BigDecimal.valueOf(Math.exp(-1d * theta.dot(x)))),
        MathContext.DECIMAL128);
  }

  private BigDecimal ln(BigDecimal x) {
    if (x.equals(BigDecimal.ONE)) {
      return BigDecimal.ZERO;
    }
    x = x.subtract(BigDecimal.ONE);
    int iterations = 1000;
    BigDecimal ret = new BigDecimal(iterations + 1);
    for (long i = iterations; i >= 0; i--) {
      BigDecimal N = new BigDecimal(i / 2 + 1).pow(2);
      N = N.multiply(x, MathContext.DECIMAL128);
      ret = N.divide(ret, MathContext.DECIMAL128);

      N = new BigDecimal(i + 1);
      ret = ret.add(N, MathContext.DECIMAL128);

    }
    ret = x.divide(ret, MathContext.DECIMAL128);
    return ret;
  }

  @Override
  public double calculateCostForItem(DoubleVector x, double y, int m,
      DoubleVector theta) {
    return costFunction.calculateCostForItem(x, y, m, theta, this);
  }
}
