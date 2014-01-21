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

package org.apache.hama.pipes;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.KeyValueTextInputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.queue.DiskQueue;
import org.apache.hama.commons.io.PipesKeyValueWritable;
import org.apache.hama.commons.io.PipesVectorWritable;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;

/**
 * Test case for {@link PipesBSP}
 * 
 */
public class TestPipes extends HamaCluster {
  private static final Log LOG = LogFactory.getLog(TestPipes.class);

  public static final String EXAMPLES_INSTALL_PROPERTY = "hama.pipes.examples.install";
  public static final String EXAMPLE_SUMMATION_EXEC = "/examples/summation";
  public static final String EXAMPLE_PIESTIMATOR_EXEC = "/examples/piestimator";
  public static final String EXAMPLE_MATRIXMULTIPLICATION_EXEC = "/examples/matrixmultiplication";
  public static final String EXAMPLE_TMP_OUTPUT = "/tmp/test-example/";
  public static final String HAMA_TMP_OUTPUT = "/tmp/hama-pipes/";
  public static final String HAMA_TMP_DISK_QUEUE_OUTPUT = "/tmp/messageQueue";
  public static final int DOUBLE_PRECISION = 6;

  private HamaConfiguration configuration;
  private static FileSystem fs = null;
  private String examplesInstallPath;

  public TestPipes() {
    configuration = new HamaConfiguration();

    try {
      // Cleanup temp Hama locations
      fs = FileSystem.get(configuration);
      cleanup(fs, new Path(HAMA_TMP_OUTPUT));
      cleanup(fs, new Path(HAMA_TMP_DISK_QUEUE_OUTPUT));
      // Remove local temp folder
      cleanup(fs, new Path(EXAMPLE_TMP_OUTPUT));
    } catch (IOException e) {
      e.printStackTrace();
    }

    configuration.set("bsp.master.address", "localhost");
    configuration.set("hama.child.redirect.log.console", "true");
    assertEquals("Make sure master addr is set to localhost:", "localhost",
        configuration.get("bsp.master.address"));
    configuration.set("bsp.local.dir", HAMA_TMP_OUTPUT);
    configuration
        .set(DiskQueue.DISK_QUEUE_PATH_KEY, HAMA_TMP_DISK_QUEUE_OUTPUT);
    configuration.set(Constants.ZOOKEEPER_QUORUM, "localhost");
    configuration.setInt(Constants.ZOOKEEPER_CLIENT_PORT, 21810);
    configuration.set("hama.sync.client.class",
        org.apache.hama.bsp.sync.ZooKeeperSyncClientImpl.class
            .getCanonicalName());
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void testPipes() throws Exception {
    assertNotNull("System property " + EXAMPLES_INSTALL_PROPERTY
        + " is not defined!", System.getProperty(EXAMPLES_INSTALL_PROPERTY));

    if (System.getProperty(EXAMPLES_INSTALL_PROPERTY).isEmpty()) {
      LOG.error("System property " + EXAMPLES_INSTALL_PROPERTY
          + " is empty! Skipping TestPipes!");
      return;
    }
    this.examplesInstallPath = System.getProperty(EXAMPLES_INSTALL_PROPERTY);

    // *** Summation Test ***
    summation();

    // *** PiEstimator Test ***
    piestimation();

    // *** MatrixMultiplication Test ***
    matrixMult();

    // Remove local temp folder
    cleanup(fs, new Path(EXAMPLE_TMP_OUTPUT));
  }

  private void summation() throws Exception {
    // Setup Paths
    Path summationExec = new Path(this.examplesInstallPath
        + EXAMPLE_SUMMATION_EXEC);
    Path inputPath = new Path(EXAMPLE_TMP_OUTPUT + "summation/in");
    Path outputPath = new Path(EXAMPLE_TMP_OUTPUT + "summation/out");

    // Generate Summation input
    BigDecimal sum = writeSummationInputFile(fs, inputPath);

    // Run Summation example
    runProgram(getSummationJob(configuration), summationExec, inputPath,
        outputPath, 1, this.numOfGroom);

    // Verify output
    verifyOutput(configuration, outputPath, sum.doubleValue(),
        Math.pow(10, (DOUBLE_PRECISION * -1)));

    // Clean input and output folder
    cleanup(fs, inputPath);
    cleanup(fs, outputPath);
  }

  private void piestimation() throws Exception {
    // Setup Paths
    Path piestimatorExec = new Path(this.examplesInstallPath
        + EXAMPLE_PIESTIMATOR_EXEC);
    Path inputPath = new Path(EXAMPLE_TMP_OUTPUT + "piestimator/in");
    Path outputPath = new Path(EXAMPLE_TMP_OUTPUT + "piestimator/out");

    // Run PiEstimator example
    runProgram(getPiestimatorJob(configuration), piestimatorExec, inputPath,
        outputPath, 3, this.numOfGroom);

    // Verify output
    verifyOutput(configuration, outputPath, Math.PI, Math.pow(10, (2 * -1)));

    // Clean input and output folder
    cleanup(fs, inputPath);
    cleanup(fs, outputPath);
  }

  private void matrixMult() throws Exception {
    // Setup Paths
    Path matrixmultiplicationExec = new Path(this.examplesInstallPath
        + EXAMPLE_MATRIXMULTIPLICATION_EXEC);

    Path inputPath = new Path(EXAMPLE_TMP_OUTPUT + "matmult/in");
    Path outputPath = new Path(EXAMPLE_TMP_OUTPUT + "matmult/out");

    // Generate matrix dimensions
    Random rand = new Random();
    // (0-19) + 11 -> between 11-30
    int rows = rand.nextInt(20) + 11;
    int cols = rand.nextInt(20) + 11;

    // Generate MatrixMultiplication input
    double[][] matrixA = createRandomMatrix(rows, cols, rand);
    double[][] matrixB = createRandomMatrix(cols, rows, rand);

    Path matrixAPath = writeMatrix(configuration, matrixA, new Path(inputPath,
        "matrixA.seq"), false);
    Path transposedMatrixBPath = writeMatrix(configuration, matrixB, new Path(
        inputPath, "transposedMatrixB.seq"), true);

    // Run MatrixMultiplication example
    runProgram(
        getMatrixMultiplicationJob(configuration, transposedMatrixBPath),
        matrixmultiplicationExec, matrixAPath, outputPath, 2, this.numOfGroom);

    // Verify output
    double[][] matrixC = multiplyMatrix(matrixA, matrixB);
    verifyMatrixMultiplicationOutput(configuration, outputPath, matrixC);

    cleanup(fs, inputPath);
    cleanup(fs, outputPath);
  }

  static BSPJob getSummationJob(HamaConfiguration conf) throws IOException {
    BSPJob bsp = new BSPJob(conf);
    bsp.setInputFormat(KeyValueTextInputFormat.class);
    bsp.setInputKeyClass(Text.class);
    bsp.setInputValueClass(Text.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(NullWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setMessageClass(DoubleWritable.class);
    return bsp;
  }

  static BSPJob getPiestimatorJob(HamaConfiguration conf) throws IOException {
    BSPJob bsp = new BSPJob(conf);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(NullWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setMessageClass(IntWritable.class);
    return bsp;
  }

  static BSPJob getMatrixMultiplicationJob(HamaConfiguration conf,
      Path transposedMatrixB) throws IOException {
    BSPJob bsp = new BSPJob(conf);
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setInputKeyClass(IntWritable.class);
    bsp.setInputValueClass(PipesVectorWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(PipesVectorWritable.class);
    bsp.setMessageClass(PipesKeyValueWritable.class);

    bsp.set(Constants.RUNTIME_PARTITIONING_DIR, HAMA_TMP_OUTPUT + "/parts");
    bsp.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, true);
    bsp.setPartitioner(PipesPartitioner.class);

    // sort sent messages
    bsp.set(MessageManager.RECEIVE_QUEUE_TYPE_CLASS,
        "org.apache.hama.bsp.message.queue.SortedMemoryQueue");
    bsp.set("hama.mat.mult.B.path", transposedMatrixB.toString());
    return bsp;
  }

  static BigDecimal writeSummationInputFile(FileSystem fs, Path dir)
      throws IOException {
    DataOutputStream out = fs.create(new Path(dir, "part0"));
    Random rand = new Random();
    double rangeMin = 0;
    double rangeMax = 100;
    BigDecimal sum = new BigDecimal(0);
    // loop between 50 and 149 times
    for (int i = 0; i < rand.nextInt(100) + 50; i++) {
      // generate key value pair inputs
      double randomValue = rangeMin + (rangeMax - rangeMin) * rand.nextDouble();
      String truncatedValue = new BigDecimal(randomValue).setScale(
          DOUBLE_PRECISION, BigDecimal.ROUND_DOWN).toString();

      String line = "key" + (i + 1) + "\t" + truncatedValue + "\n";
      out.writeBytes(line);

      sum = sum.add(new BigDecimal(truncatedValue));
    }
    out.close();
    return sum;
  }

  static double[][] createRandomMatrix(int rows, int columns, Random rand) {
    final double[][] matrix = new double[rows][columns];
    double rangeMin = 0;
    double rangeMax = 100;

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < columns; j++) {
        double randomValue = rangeMin + (rangeMax - rangeMin)
            * rand.nextDouble();
        matrix[i][j] = new BigDecimal(randomValue).setScale(DOUBLE_PRECISION,
            BigDecimal.ROUND_DOWN).doubleValue();
      }
    }
    return matrix;
  }

  static Path writeMatrix(Configuration conf, double[][] matrix, Path path,
      boolean saveTransposed) {
    // Write matrix to DFS
    SequenceFile.Writer writer = null;
    try {
      writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class,
          PipesVectorWritable.class);

      // Transpose Matrix before saving
      if (saveTransposed) {
        int rows = matrix.length;
        int columns = matrix[0].length;
        double[][] transposed = new double[columns][rows];
        for (int i = 0; i < rows; i++) {
          for (int j = 0; j < columns; j++) {
            transposed[j][i] = matrix[i][j];
          }
        }
        matrix = transposed;
      }

      LOG.info("writeRandomDistributedRowMatrix path: " + path
          + " saveTransposed: " + saveTransposed);
      for (int i = 0; i < matrix.length; i++) {
        DenseDoubleVector rowVector = new DenseDoubleVector(matrix[i]);
        writer.append(new IntWritable(i), new PipesVectorWritable(rowVector));
        LOG.info("IntWritable: " + i + " PipesVectorWritable: "
            + rowVector.toString());
      }

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return path;
  }

  static double[][] multiplyMatrix(double[][] matrixA, double[][] matrixB) {
    final double[][] matrixC = new double[matrixA.length][matrixB[0].length];
    int m = matrixA.length;
    int n = matrixA[0].length;
    int p = matrixB[0].length;

    for (int k = 0; k < n; k++) {
      for (int i = 0; i < m; i++) {
        for (int j = 0; j < p; j++) {
          matrixC[i][j] = matrixC[i][j] + matrixA[i][k] * matrixB[k][j];
        }
      }
    }
    return matrixC;
  }

  static void verifyOutput(HamaConfiguration conf, Path outputPath,
      String[] expectedResults) throws IOException {
    FileStatus[] listStatus = fs.listStatus(outputPath);
    for (FileStatus status : listStatus) {
      if (!status.isDir()) {
        if (status.getLen() > 0) {
          LOG.info("Output File: " + status.getPath());
          BufferedReader br = new BufferedReader(new InputStreamReader(
              fs.open(status.getPath())));
          try {
            String line = "";
            int i = 0;
            while ((line = br.readLine()) != null) {
              LOG.info("output[" + i + "]: '" + line + "'");
              LOG.info("expected[" + i + "]: '" + expectedResults[i] + "'");
              assertEquals("'" + expectedResults[i] + "' != '" + line + "'",
                  expectedResults[i], line);
              i++;
            }
          } finally {
            br.close();
          }
        }
      }
    }
  }

  static void verifyOutput(HamaConfiguration conf, Path outputPath,
      double expectedResult, double delta) throws IOException {
    FileStatus[] listStatus = fs.listStatus(outputPath);
    for (FileStatus status : listStatus) {
      if (!status.isDir()) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
            status.getPath(), conf);
        NullWritable key = NullWritable.get();
        DoubleWritable value = new DoubleWritable();
        if (reader.next(key, value)) {
          LOG.info("Output File: " + status.getPath());
          LOG.info("key: '" + key + "' value: '" + value + "' expected: '"
              + expectedResult + "'");
          assertEquals("Expected value: '" + expectedResult + "' != '" + value
              + "'", expectedResult, value.get(), delta);
        }
        reader.close();
      }
    }
  }

  static void verifyMatrixMultiplicationOutput(HamaConfiguration conf,
      Path outputPath, double[][] matrix) throws IOException {
    FileStatus[] listStatus = fs.listStatus(outputPath);
    for (FileStatus status : listStatus) {
      if (!status.isDir()) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
            status.getPath(), conf);
        IntWritable key = new IntWritable();
        PipesVectorWritable value = new PipesVectorWritable();
        int rowIdx = 0;
        while (reader.next(key, value)) {
          assertEquals("Expected rowIdx: '" + rowIdx + "' != '" + key.get()
              + "'", rowIdx, key.get());

          DoubleVector rowVector = value.getVector();

          for (int colIdx = 0; colIdx < rowVector.getLength(); colIdx++) {
            double colValue = rowVector.get(colIdx);
            assertEquals("Expected colValue: '" + matrix[rowIdx][colIdx]
                + "' != '" + colValue + "' in row: " + rowIdx + " values: "
                + rowVector.toString(), matrix[rowIdx][colIdx], colValue,
                Math.pow(10, (DOUBLE_PRECISION * -1)));
          }
          rowIdx++;
        }
        reader.close();
      }
    }
  }

  static void cleanup(FileSystem fs, Path p) throws IOException {
    fs.delete(p, true);
    assertFalse(p.getName() + " not cleaned up", fs.exists(p));
  }

  static void runProgram(BSPJob bsp, Path program, Path inputPath,
      Path outputPath, int numBspTasks, int numOfGroom) throws IOException,
      ClassNotFoundException, InterruptedException {
    HamaConfiguration conf = (HamaConfiguration) bsp.getConfiguration();
    bsp.setJobName("Test Hama Pipes " + program.getName());
    bsp.setBspClass(PipesBSP.class);

    FileInputFormat.setInputPaths(bsp, inputPath);
    FileOutputFormat.setOutputPath(bsp, outputPath);

    BSPJobClient jobClient = new BSPJobClient(conf);

    // Set bspTaskNum
    ClusterStatus cluster = jobClient.getClusterStatus(false);
    assertEquals(numOfGroom, cluster.getGroomServers());
    bsp.setNumBspTask(numBspTasks);

    // Copy binary to DFS
    Path testExec = new Path(EXAMPLE_TMP_OUTPUT + "testing/bin/application");
    fs.delete(testExec.getParent(), true);
    fs.copyFromLocalFile(program, testExec);

    // Set Executable
    Submitter.setExecutable(conf, fs.makeQualified(testExec).toString());

    // Run bspJob
    Submitter.runJob(bsp);

    LOG.info("Client finishes execution job");

    // check output
    FileStatus[] listStatus = fs.listStatus(outputPath);
    assertEquals(listStatus.length, numBspTasks);
  }
}
