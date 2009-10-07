package org.apache.hama;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.Matrix;
import org.apache.log4j.Logger;

public class TestHamaAdmin extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestHamaAdmin.class);
  private int SIZE = 10;
  private Matrix m1;
  private Matrix m2;
  private final String aliase1 = "matrix_aliase_A";
  private final String aliase2 = "matrix_aliase_B";
  private HamaConfiguration conf;
  private HBaseAdmin admin;
  private HamaAdmin hamaAdmin;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestHamaAdmin() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();
    admin = new HBaseAdmin(conf);
    hamaAdmin = new HamaAdminImpl(conf, admin);

    m1 = DenseMatrix.random(conf, SIZE, SIZE);
    m2 = DenseMatrix.random(conf, SIZE, SIZE);
  }
  
  public void testLoadSave() throws IOException {
    String path1 = m1.getPath();
    // save m1 to aliase1
    m1.save(aliase1);
    // load matrix m1 using aliase1
    DenseMatrix loadTest = new DenseMatrix(conf, aliase1, false);

    for (int i = 0; i < SIZE; i++) {
      for (int j = 0; j < SIZE; j++) {
        assertEquals(m1.get(i, j), loadTest.get(i, j));
      }
    }

    assertEquals(path1, loadTest.getPath());
    // close loadTest, it just disconnect to the table but didn't delete it.
    loadTest.close();

    // try to close m1 & load matrix m1 using aliase1 again.
    m1.close();
    DenseMatrix loadTest2 = new DenseMatrix(conf, aliase1, false);
    assertEquals(path1, loadTest2.getPath());
    // remove aliase1
    // because loadTest2 connect the aliase1, so we just remove aliase entry
    // but didn't delete the table.
    hamaAdmin.delete(aliase1);
    assertEquals(true, admin.tableExists(path1));
    // close loadTest2, because it is the last one who reference table 'path1'
    // it will do the gc!
    loadTest2.close();
    assertEquals(false, admin.tableExists(path1));

    // if we try to load non-existed matrix using aliase name, it should fail.
    DenseMatrix loadTest3 = null;
    try {
      loadTest3 = new DenseMatrix(conf, aliase1, false);
      fail("Try to load a non-existed matrix should fail!");
    } catch (IOException e) {

    } finally {
      if (loadTest3 != null)
        loadTest3.close();
    }
    
    forceCreate();
  }

  public void forceCreate() throws IOException {
    String path2 = m2.getPath();
    // save m2 to aliase2
    m2.save(aliase2);
    // load matrix m2 using aliase2
    DenseMatrix loadTest = new DenseMatrix(conf, aliase2, false);

    for (int i = 0; i < loadTest.getRows(); i++) {
      for (int j = 0; j < loadTest.getColumns(); j++) {
        assertEquals(m2.get(i, j), loadTest.get(i, j));
      }
    }

    assertEquals(path2, loadTest.getPath());

    Matrix test = hamaAdmin.getMatrix(aliase2);
    assertEquals(test.getType(), "DenseMatrix");

    // force to create matrix loadTest2 using aliasename 'aliase2'
    DenseMatrix loadTest2 = new DenseMatrix(conf, aliase2, true);
    String loadPath2 = loadTest2.getPath();
    assertFalse(path2.equals(loadPath2));
    assertEquals(loadPath2, hamaAdmin.getPath(aliase2));
    assertFalse(path2.equals(hamaAdmin.getPath(aliase2)));

    // try to close m2 & loadTest, it table will be deleted finally
    m2.close();
    assertEquals(true, admin.tableExists(path2));
    loadTest.close();
    assertEquals(false, admin.tableExists(path2));

    // remove 'aliase2' & close loadTest2
    loadTest2.close();
    assertEquals(true, admin.tableExists(loadPath2));
    hamaAdmin.delete(aliase2);
    assertEquals(false, admin.tableExists(loadPath2));
  }
}
