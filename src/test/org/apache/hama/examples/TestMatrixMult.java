package org.apache.hama.examples;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.Matrix;

public class TestMatrixMult extends HamaCluster {
  private int SIZE = 8;
  private Matrix m1;
  private Matrix m2;
  private HamaConfiguration conf;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestMatrixMult() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();

    m1 = RandomMatrix.random_mapred(conf, SIZE, SIZE);
    m2 = RandomMatrix.random_mapred(conf, SIZE, SIZE);
  }

  /**
   * Test matrices multiplication
   * 
   * @throws IOException
   */
  public void testMult() throws IOException {
    Matrix result = MatrixMultiplication.mult(m1, m2);

    assertEquals(result.getRows(), SIZE);
    assertEquals(result.getColumns(), SIZE);

    Matrix result2 = MatrixMultiplication.mult(m1, m2, 4);
    
    verifyMultResult(m1, m2, result);
    verifyMultResult(m1, m2, result2);
  }

  /**
   * Verifying multiplication result
   * 
   * @param m1
   * @param m2
   * @param result
   * @throws IOException
   */
  private void verifyMultResult(Matrix m1, Matrix m2, Matrix result)
      throws IOException {
    double[][] c = new double[SIZE][SIZE];

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        for (int k = 0; k < SIZE; k++) {
          c[i][k] += m1.get(i, j) * m2.get(j, k);
        }
      }
    }

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertTrue((Math.abs(c[i][j] - result.get(i, j)) < .0000001));
      }
    }
  }
}
