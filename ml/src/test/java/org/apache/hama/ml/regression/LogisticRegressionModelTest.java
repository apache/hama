package org.apache.hama.ml.regression;

import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    Double cost = logisticRegressionModel.calculateCostForItem(x, y, 2, theta);
    assertEquals("wrong cost calculation for logistic regression", Double.valueOf(16d), cost);
  }

  @Test
  public void testCorrectHypothesisCalculation() throws Exception {
    LogisticRegressionModel logisticRegressionModel = new LogisticRegressionModel();
    Double hypothesisValue = logisticRegressionModel.applyHypothesis(new DenseDoubleVector(new double[]{1, 1, 1}),
            new DenseDoubleVector(new double[]{2, 3, 4}));
    assertEquals("wrong hypothesis value for logistic regression", Double.valueOf(0.9998766054240137), hypothesisValue);
  }
}
