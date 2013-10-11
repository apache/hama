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

  private static final MathContext DEFAULT_PRECISION = MathContext.DECIMAL128;

  private final CostFunction costFunction;

  public LogisticRegressionModel() {
    costFunction = new CostFunction() {
      @Override
      public BigDecimal calculateCostForItem(DoubleVector x, double y, int m, DoubleVector theta,
              HypothesisFunction hypothesis) {
        // -1/m*(y*ln(hx) + (1-y)*ln(1-hx))
        BigDecimal hx = applyHypothesisWithPrecision(theta, x);
        BigDecimal first = BigDecimal.valueOf(y).multiply(ln(hx));
        BigDecimal logarg = BigDecimal.valueOf(1).subtract(hx, DEFAULT_PRECISION);
        BigDecimal ln = ln(logarg);
        BigDecimal second = BigDecimal.valueOf(1d - y).multiply(ln);
        BigDecimal num = first.add(second);
        BigDecimal den = BigDecimal.valueOf(-1*m);
        BigDecimal res = num.divide(den, DEFAULT_PRECISION);
        return res;
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
    double d = Math.exp(dotUnsafe);
    BigDecimal exp = BigDecimal.valueOf(d);
    BigDecimal den = BigDecimal.valueOf(1d).add(exp);
    BigDecimal remainder = BigDecimal.valueOf(1).subtract(den, DEFAULT_PRECISION);
    BigDecimal res = BigDecimal.valueOf(1).divide(den, DEFAULT_PRECISION);
    if (res.doubleValue() == 1 && remainder.doubleValue() < 0) {
      res = res.add(remainder);
    }
    return res;
  }

  private BigDecimal ln(BigDecimal x) {
//    if (x.equals(BigDecimal.ONE)) {
//      return BigDecimal.ZERO;
//    }
//    x = x.subtract(BigDecimal.ONE);
//    int iterations = 10000000;
//    BigDecimal ret = new BigDecimal(iterations + 1);
//    for (long i = iterations; i >= 0; i--) {
//      BigDecimal N = new BigDecimal(i / 2 + 1).pow(2);
//      N = N.multiply(x, DEFAULT_PRECISION);
//      ret = N.divide(ret, DEFAULT_PRECISION);
//
//      N = new BigDecimal(i + 1);
//      ret = ret.add(N, DEFAULT_PRECISION);
//
//    }
//    ret = x.divide(ret, DEFAULT_PRECISION);
//    return ret;
    return BigDecimal.valueOf(Math.log(x.doubleValue()));
  }

  @Override
  public BigDecimal calculateCostForItem(DoubleVector x, double y, int m, DoubleVector theta) {
    return costFunction.calculateCostForItem(x, y, m, theta, this);
  }
}
