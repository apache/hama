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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.math.BigDecimal;

import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.junit.Test;

/**
 * Testcase for {@link LogisticRegressionModel}
 */
public class LogisticRegressionModelTest {

  @Test
  public void testCorrectCostCalculation() throws Exception {
    LogisticRegressionModel logisticRegressionModel = new LogisticRegressionModel();
    DoubleVector x = new DenseDoubleVector(new double[]{2, 3, 4});
    double y = 1;
    DoubleVector theta = new DenseDoubleVector(new double[]{1, 1, 1});
    BigDecimal cost = logisticRegressionModel.calculateCostForItem(x, y, 2, theta);
    assertEquals("wrong cost calculation for logistic regression", 6.170109486162941E-5d, cost.doubleValue(), 0.000001);
  }

  @Test
  public void testCorrectHypothesisCalculation() throws Exception {
    LogisticRegressionModel logisticRegressionModel = new LogisticRegressionModel();
    BigDecimal hypothesisValue = logisticRegressionModel.applyHypothesis(new DenseDoubleVector(new double[]{1, 1, 1}),
            new DenseDoubleVector(new double[]{2, 3, 4}));
    assertEquals("wrong hypothesis value for logistic regression", 0.9998766054240137682597533152954043d, hypothesisValue.doubleValue(), 0.000001);
  }
  
  @Test
  public void testMultipleCostCalculation() throws Exception {
    LogisticRegressionModel logisticRegressionModel = new LogisticRegressionModel();
    double[] theta1 = new double[] { 10.010000000474975, 10.050000002374873, 10.01600000075996,
        10.018000000854954, 10.024000001139939, 10.038000001804903, 10.036000001709908 };
    double[] theta2 = new double[] { 13.000000142492354, 25.00000071246177, 14.800000227987766,
        15.400000256486237, 17.20000034198165, 21.400000541470945, 20.800000512972474 };

    DenseDoubleVector theta1Vector = new DenseDoubleVector(theta1);
    DenseDoubleVector theta2Vector = new DenseDoubleVector(theta2);

    DenseDoubleVector x = new DenseDoubleVector(new double[] { 1, 10, 3, 2, 1, 6, 1 });

    BigDecimal res1 = logisticRegressionModel.applyHypothesis(theta1Vector, x);
    BigDecimal res2 = logisticRegressionModel.applyHypothesis(theta2Vector, x);

    assertFalse(res1 + " shouldn't be the same as " + res2, res1.equals(res2));

    BigDecimal itemCost1 = logisticRegressionModel.calculateCostForItem(x, 2, 8, theta1Vector);
    BigDecimal itemCost2 = logisticRegressionModel.calculateCostForItem(x, 2, 8, theta2Vector);

    assertFalse(itemCost1 + " shouldn't be the same as " + itemCost2, itemCost1.equals(itemCost2));

  }
}
