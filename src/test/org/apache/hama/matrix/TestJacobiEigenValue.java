package org.apache.hama.matrix;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.log4j.Logger;

public class TestJacobiEigenValue extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestDenseMatrix.class);
  private int SIZE = 10;
  private Matrix m5;
  private HamaConfiguration conf;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestJacobiEigenValue() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();
    m5 = DenseMatrix.random(conf, SIZE, SIZE);
  }

  public void testJacobiEigenValue() throws IOException {
    // copy Matrix m5 to the array
    double[][] S = new double[SIZE][SIZE];

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        S[i][j] = m5.get(i, j);
      }
    }

    // do m/r jacobi eigen value computation
    DenseMatrix dm = (DenseMatrix) m5;
    dm.jacobiEigenValue(3);

    // do jacobi egien value over S array
    int i, j, k, l, m, state;
    double s, c, t, p, y;
    double e1, e2;
    // index array
    int[] ind = new int[SIZE];
    boolean[] changed = new boolean[SIZE];

    // output
    double[] e = new double[SIZE];
    double[][] E = new double[SIZE][SIZE];

    // init e & E; ind & changed
    for (i = 0; i < SIZE; i++) {
      for (j = 0; j < SIZE; j++) {
        E[i][j] = 0;
      }
      E[i][i] = 1;
    }

    state = SIZE;

    for (i = 0; i < SIZE; i++) {
      ind[i] = maxind(S, i, SIZE);
      e[i] = S[i][i];
      changed[i] = true;
    }

    int loops = 3;
    // next rotation
    while (state != 0 && loops > 0) {
      // find index(k, l) for pivot p
      m = 0;
      for (k = 1; k <= SIZE - 2; k++) {
        if (Math.abs(S[m][ind[m]]) < Math.abs(S[k][ind[k]])) {
          m = k;
        }
      }

      k = m;
      l = ind[m];
      p = S[k][l];

      // calculate c = cos, s = sin
      y = (e[l] - e[k]) / 2;
      t = Math.abs(y) + Math.sqrt(p * p + y * y);
      s = Math.sqrt(p * p + t * t);
      c = t / s;
      s = p / s;
      t = (p * p) / t;
      if (y < 0) {
        s = -s;
        t = -t;
      }

      S[k][l] = 0.0;
      state = update(e, changed, k, -t, state);
      state = update(e, changed, l, t, state);

      for (i = 0; i <= k - 1; i++)
        rotate(S, i, k, i, l, c, s);
      for (i = l + 1; i < SIZE; i++)
        rotate(S, k, i, l, i, c, s);
      for (i = k + 1; i <= l - 1; i++)
        rotate(S, k, i, i, l, c, s);

      // rotate eigenvectors
      for (i = 0; i < SIZE; i++) {
        e1 = E[k][i];
        e2 = E[l][i];

        E[k][i] = c * e1 - s * e2;
        E[l][i] = s * e1 + c * e2;
      }

      ind[k] = maxind(S, k, SIZE);
      ind[l] = maxind(S, l, SIZE);

      loops--;
    }

    // verify the results
    assertTrue(dm.verifyEigenValue(e, E));
  }

  // index of largest off-diagonal element in row k
  int maxind(double[][] S, int row, int size) {
    int m = row + 1;
    for (int i = row + 2; i < size; i++) {
      if (Math.abs(S[row][i]) > Math.abs(S[row][m]))
        m = i;
    }
    return m;
  }

  int update(double[] e, boolean[] changed, int row, double value, int state) {
    double y = e[row];
    e[row] += value;

    if (changed[row] && y == e[row]) {
      changed[row] = false;
      return state - 1;
    } else if (!changed[row] && y != e[row]) {
      changed[row] = true;
      return state + 1;
    } else
      return state;
  }

  void rotate(double[][] S, int k, int l, int i, int j, double c, double s) {
    double s1 = S[k][l], s2 = S[i][j];
    S[k][l] = c * s1 - s * s2;
    S[i][j] = s * s1 + c * s2;
  }
}
