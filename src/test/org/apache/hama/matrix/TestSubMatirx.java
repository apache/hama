package org.apache.hama.matrix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.log4j.Logger;

public class TestSubMatirx extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestSubMatirx.class);
  private int SIZE = 10;
  private Matrix m1;
  private Matrix m2;
  private HamaConfiguration conf;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestSubMatirx() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();

    m1 = DenseMatrix.random(conf, SIZE, SIZE);
    m2 = DenseMatrix.random(conf, SIZE, SIZE);
  }
  
  public void testSubMatrix() throws IOException {
    SubMatrix a = m1.subMatrix(2, 4, 2, 5); // A : 3 * 4
    for (int i = 0; i < a.getRows(); i++) {
      for (int j = 0; j < a.getColumns(); j++) {
        assertEquals(a.get(i, j), m1.get(i + 2, j + 2));
      }
    }

    SubMatrix b = m2.subMatrix(0, 3, 0, 2); // B : 4 * 3
    SubMatrix c = a.mult(b);

    double[][] C = new double[3][3]; // A * B
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 4; k++) {
          C[i][j] += m1.get(i + 2, k + 2) * m2.get(k, j);
        }
      }
    }

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        assertEquals(C[i][j], c.get(i, j));
      }
    }
  }
}
