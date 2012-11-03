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

import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Testcase for {@link LinearRegressionModel}
 */
public class LinearRegressionModelTest {

  @Test
  public void testCorrectCostCalculation() throws Exception {
    LinearRegressionModel linearRegressionModel = new LinearRegressionModel();
    DoubleVector x = new DenseDoubleVector(new double[]{2, 3, 4});
    double y = 1;
    DoubleVector theta = new DenseDoubleVector(new double[]{1, 1, 1});
    Double cost = linearRegressionModel.calculateCostForItem(x, y, 2, theta);
    assertEquals("wrong cost calculation for linear regression", Double.valueOf(16d), cost);
  }

  @Test
  public void testCorrectHypothesisCalculation() throws Exception {
    LinearRegressionModel linearRegressionModel = new LinearRegressionModel();
    Double hypothesisValue = linearRegressionModel.applyHypothesis(new DenseDoubleVector(new double[]{1, 1, 1}),
            new DenseDoubleVector(new double[]{2, 3, 4}));
    assertEquals("wrong hypothesis value for linear regression", Double.valueOf(9), hypothesisValue);
  }
}
