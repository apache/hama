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

import org.apache.hama.commons.math.DoubleVector;

/**
 * A {@link RegressionModel} for logistic regression
 */
public class LogisticRegressionModel implements RegressionModel {

  private static final MathContext DEFAULT_PRECISION = MathContext.DECIMAL128;

  private final CostFunction costFunction;

  public LogisticRegressionModel() {
    costFunction = new CostFunction() {
      @Override
      public BigDecimal calculateCostForItem(DoubleVector x, double y, int m, DoubleVector theta,
                                             HypothesisFunction hypothesis) {
        // -1/m*(y*ln(hx) + (1-y)*ln(1-hx))
        BigDecimal hx = applyHypothesisWithPrecision(theta, x);
        BigDecimal firstTerm = BigDecimal.valueOf(y).multiply(ln(hx));
        BigDecimal secondTerm = BigDecimal.valueOf(1d - y).multiply(ln(BigDecimal.valueOf(1).subtract(hx, DEFAULT_PRECISION)));
        BigDecimal num = firstTerm.add(secondTerm);
        BigDecimal den = BigDecimal.valueOf(-1 * m);
        return num.divide(den, DEFAULT_PRECISION);
      }
    };
  }

  @Override
  public BigDecimal applyHypothesis(DoubleVector theta, DoubleVector x) {
    return applyHypothesisWithPrecision(theta, x);
  }

  private BigDecimal applyHypothesisWithPrecision(DoubleVector theta, DoubleVector x) {
    // 1 / (1 + (e^(-theta'x)))
    double dotUnsafe = theta.multiply(-1d).dotUnsafe(x);
    BigDecimal den = BigDecimal.valueOf(1d).add(BigDecimal.valueOf(Math.exp(dotUnsafe)));
    BigDecimal res = BigDecimal.valueOf(1).divide(den, DEFAULT_PRECISION);
    BigDecimal remainder = BigDecimal.valueOf(1).subtract(den, DEFAULT_PRECISION);
    if (res.doubleValue() == 1 && remainder.doubleValue() < 0) {
      res = res.add(remainder);
    }
    return res;
  }

  private BigDecimal ln(BigDecimal x) {
    // TODO : implement this using proper logarithm for BigDecimals
    return BigDecimal.valueOf(Math.log(x.doubleValue()));
  }

  @Override
  public BigDecimal calculateCostForItem(DoubleVector x, double y, int m, DoubleVector theta) {
    return costFunction.calculateCostForItem(x, y, m, theta, this);
  }
}
