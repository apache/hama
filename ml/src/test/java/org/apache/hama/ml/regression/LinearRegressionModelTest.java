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
  public void testCorrectCalculation() throws Exception {
    LinearRegressionModel linearRegressionModel = new LinearRegressionModel();
    DoubleVector x = new DenseDoubleVector(new double[]{2, 3, 4});
    double y = 1;
    DoubleVector theta = new DenseDoubleVector(new double[]{1, 1, 1});
    Double cost = linearRegressionModel.calculateCostForItem(x, y, 2, theta);
    assertEquals("wrong cost calculation for linear regression", Double.valueOf(16d), cost);
  }
}
