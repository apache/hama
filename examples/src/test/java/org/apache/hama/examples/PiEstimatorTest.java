package org.apache.hama.examples;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * @author tommaso
 */
public class PiEstimatorTest {
  @Test
  public void testCorrectPiExecution() {
    try {
      PiEstimator.main(new String[]{"10"});
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
  }

  @Test
  public void testPiExecutionWithEmptyArgs() {
    try {
      PiEstimator.main(new String[10]);
      fail("PiEstimator should fail if the argument list has size 0");
    } catch (Exception e) {
      // everything ok
    }
  }

}
