package org.apache.hama;

import java.io.File;
import java.io.IOException;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hama.util.Bytes;

public abstract class HamaTestCase extends TestCase {
  private static Log LOG = LogFactory.getLog(HamaTestCase.class);
  
  /** configuration parameter name for test directory */
  public static final String TEST_DIRECTORY_KEY = "test.build.data";

  private boolean localfs = false;
  protected Path testDir = null;
  protected FileSystem fs = null;
  
  static {
    initialize();
  }

  public volatile HamaConfiguration conf;

  /** constructor */
  public HamaTestCase() {
    super();
    init();
  }
  
  /**
   * @param name
   */
  public HamaTestCase(String name) {
    super(name);
    init();
  }
  
  private void init() {
    conf = new HamaConfiguration();
  }

  /**
   * Note that this method must be called after the mini hdfs cluster has
   * started or we end up with a local file system.
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    localfs =
      (conf.get("fs.defaultFS", "file:///").compareTo("file:///") == 0);

    if (fs == null) {
      this.fs = FileSystem.get(conf);
    }
    try {
      if (localfs) {
        this.testDir = getUnitTestdir(getName());
        if (fs.exists(testDir)) {
          fs.delete(testDir, true);
        }
      } else {
        this.testDir =
          this.fs.makeQualified(new Path("/tmp/hama-test"));
      }
    } catch (Exception e) {
      LOG.fatal("error during setup", e);
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      if (localfs) {
        if (this.fs.exists(testDir)) {
          this.fs.delete(testDir, true);
        }
      }
    } catch (Exception e) {
      LOG.fatal("error during tear down", e);
    }
    super.tearDown();
  }

  protected Path getUnitTestdir(String testName) {
    return new Path(
        conf.get(TEST_DIRECTORY_KEY, "test/build/data"), testName);
  }

  /**
   * Initializes parameters used in the test environment:
   *
   * Sets the configuration parameter TEST_DIRECTORY_KEY if not already set.
   * Sets the boolean debugging if "DEBUGGING" is set in the environment.
   * If debugging is enabled, reconfigures logging so that the root log level is
   * set to WARN and the logging level for the package is set to DEBUG.
   */
  public static void initialize() {
    if (System.getProperty(TEST_DIRECTORY_KEY) == null) {
      System.setProperty(TEST_DIRECTORY_KEY, new File(
          "build/hama/test").getAbsolutePath());
    }
  }

  /**
   * Common method to close down a MiniDFSCluster and the associated file system
   *
   * @param cluster
   */
  public static void shutdownDfs(MiniDFSCluster cluster) {
    if (cluster != null) {
      LOG.info("Shutting down Mini DFS ");
      try {
        cluster.shutdown();
      } catch (Exception e) {
        /// Can get a java.lang.reflect.UndeclaredThrowableException thrown
        // here because of an InterruptedException. Don't let exceptions in
        // here be cause of test failure.
      }
      try {
        FileSystem fs = cluster.getFileSystem();
        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
        FileSystem.closeAll();
      } catch (IOException e) {
        LOG.error("error closing file system", e);
      }
    }
  }

  public void assertByteEquals(byte[] expected,
                               byte[] actual) {
    if (Bytes.compareTo(expected, actual) != 0) {
      throw new AssertionFailedError("expected:<" +
      Bytes.toString(expected) + "> but was:<" +
      Bytes.toString(actual) + ">");
    }
  }

  public static void assertEquals(byte[] expected,
                               byte[] actual) {
    if (Bytes.compareTo(expected, actual) != 0) {
      throw new AssertionFailedError("expected:<" +
      Bytes.toStringBinary(expected) + "> but was:<" +
      Bytes.toStringBinary(actual) + ">");
    }
  }

}
