package org.apache.hama.shell.parser.expression;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.HCluster;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.shell.HamaShellEnv;
import org.apache.hama.shell.execution.HamaExpression;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestHamaExpressionParser extends TestCase {

  final static Log log = LogFactory.getLog(TestHamaExpressionParser.class);

  // grammar test
  static final String badExpr1 = "b + c;"; // we only accept evaluation
  // expression
  // or save expression
  static final String badExpr2 = "a = b + c"; // missing ";"
  static final String badExpr3 = "a = b + (c + d * b ;"; // missing ")"
  // grammar test and aliases test
  static final String Expr1 = "a = b;";
  static final String Expr2 = "a = b + (c+d) * b;";
  static final String Expr3 = "a = e;";
  // execution test
  // A + B : A.rows = B.rows ; A.columns = B.columns ;
  // A * B : A.columns = B.rows ;
  static final String Expr4 = "m1 = ( b + c ) * f ;"; // b.columns = c.columns =
  // f.rows
  static final String Expr4_Aliase = "m1";

  static final String Expr5 = "m2 = ( b + c ) * g ;"; // c.columns != f.rows
  static final String Expr5_Aliase = "m2";

  static final String Expr6 = "m3 = b + f ; "; // b.columns != f.columns
  static final String Expr6_Aliase = "m3";

  private static int SIZE1 = 10;
  private static int SIZE2 = 20;
  private static Matrix B;
  private static Matrix C;
  private static Matrix D;

  private static Matrix F;
  private static Matrix G;

  private static HamaConfiguration hConfig;
  private static HamaShellEnv env;

  public static Test suite() {
    TestSetup setup = new TestSetup(new TestSuite(
        TestHamaExpressionParser.class)) {
      protected void setUp() throws Exception {
        HCluster hCluster = new HCluster();
        hCluster.setUp();

        B = DenseMatrix.random(hCluster.getConf(), SIZE1, SIZE1);
        C = DenseMatrix.random(hCluster.getConf(), SIZE1, SIZE1);
        D = DenseMatrix.random(hCluster.getConf(), SIZE1, SIZE1);

        F = DenseMatrix.random(hCluster.getConf(), SIZE1, SIZE2);
        G = DenseMatrix.random(hCluster.getConf(), SIZE2, SIZE1);

        hConfig = hCluster.getConf();
        env = new HamaShellEnv();

        setUpShellEnv();
      }

      protected void tearDown() {
        log.info("tearDown()");
        try {
          close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    return setup;
  }

  private static void close() throws IOException {
    B.close();
    C.close();
    D.close();
    F.close();
    G.close();
  }

  private static void setUpShellEnv() {
    env.setAliase("b", B);
    env.setAliase("c", C);
    env.setAliase("d", D);

    env.setAliase("f", F);
    env.setAliase("g", G);
  }

  private void testBadExpression(String expression) {
    InputStream in = new ByteArrayInputStream(expression.getBytes());
    HamaExpressionParser parser = new HamaExpressionParser(in, env, hConfig);
    try {
      HamaExpression hExpression = parser.Start();
      assertNull(hExpression);
    } catch (ParseException e) {
    }
  }

  private void testNormalExpression(String expression) throws ParseException {
    InputStream in = new ByteArrayInputStream(expression.getBytes());
    HamaExpressionParser parser = new HamaExpressionParser(in, env, hConfig);
    HamaExpression hExpression = parser.Start();
    assertNotNull(hExpression);
  }

  /**
   * Expression Grammar Parser Test
   * 
   * @throws ParseException
   */
  public void testGrammar() throws ParseException {
    // badExpr1
    testBadExpression(badExpr1);
    // badExpr2
    testBadExpression(badExpr2);
    // badExpr3
    testBadExpression(badExpr3);
    // Expr1
    testNormalExpression(Expr1);
    // Expr2
    testNormalExpression(Expr2);
  }

  /**
   * Expression Aliases Test
   * 
   * we cannot use an aliases without initialization.
   * 
   * @throws ParseException
   */
  public void testAliases() throws ParseException {
    // Expr3
    testBadExpression(Expr3);
    // Expr1
    testNormalExpression(Expr1);
  }

  private void testExprExec(String expression, String matrixAliase,
      boolean isNormalExpr) throws ParseException {
    InputStream in = new ByteArrayInputStream(expression.getBytes());
    HamaExpressionParser parser = new HamaExpressionParser(in, env, hConfig);
    HamaExpression hExpression = parser.Start();
    assertNotNull(hExpression);
    hExpression.execute();
    assertEquals(env.containAliase(matrixAliase), isNormalExpr);
  }

  /**
   * Expression Execution Test
   * 
   * @throws ParseException
   */
  public void testExprExec() throws ParseException {
    // Expr4
    testExprExec(Expr4, Expr4_Aliase, true);
    // Expr5
    testExprExec(Expr5, Expr5_Aliase, false);
    // Expr6
    testExprExec(Expr6, Expr6_Aliase, false);
  }

}
