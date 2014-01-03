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
package org.apache.hama.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.DenseVectorWritable;
import org.apache.hama.commons.io.SparseVectorWritable;
import org.apache.hama.commons.util.KeyValuePair;

/**
 * Sparse matrix vector multiplication. Currently it uses row-wise access.
 * Assumptions: 1) each peer should have copy of input vector for efficient
 * operations. 2) row-wise implementation is good because we don't need to care
 * about communication 3) the main way to improve performance - create custom
 * Partitioner
 * 
 * TODO need to be simplified.
 */
public class SpMV {

  protected static final Log LOG = LogFactory.getLog(SpMV.class);
  private static String resultPath;
  private static final String outputPathString = "spmv.outputpath";
  private static final String inputMatrixPathString = "spmv.inputmatrixpath";
  private static final String inputVectorPathString = "spmv.inputvectorpath";
  private static String requestedBspTasksString = "bsptask.count";
  private static final String intermediate = "/part";

  enum RowCounter {
    TOTAL_ROWS
  }

  public static String getResultPath() {
    return resultPath;
  }

  public static void setResultPath(String resultPath) {
    SpMV.resultPath = resultPath;
  }

  /**
   * IMPORTANT: This can be a bottle neck. Problem can be here{@core
   * WritableUtil.convertSpMVOutputToDenseVector()}
   */
  private static void convertToDenseVector(HamaConfiguration conf)
      throws IOException {
    String resultPath = convertSpMVOutputToDenseVector(
        conf.get(outputPathString), conf);
    setResultPath(resultPath);
  }

  /**
   * This class performs sparse matrix vector multiplication. u = m * v.
   */
  private static class SpMVBSP
      extends
      BSP<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, NullWritable> {
    private DenseVectorWritable v;

    /**
     * Each peer reads input dense vector.
     */
    @Override
    public void setup(
        BSPPeer<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      // reading input vector, which represented as matrix row
      HamaConfiguration conf = (HamaConfiguration) peer.getConfiguration();
      v = new DenseVectorWritable();
      readFromFile(conf.get(inputVectorPathString), v, conf);
      peer.sync();
    }

    /**
     * Local inner product computation and output.
     */
    @Override
    public void bsp(
        BSPPeer<IntWritable, SparseVectorWritable, IntWritable, DoubleWritable, NullWritable> peer)
        throws IOException, SyncException, InterruptedException {
      KeyValuePair<IntWritable, SparseVectorWritable> row = null;
      while ((row = peer.readNext()) != null) {
        // it will be needed in conversion of output to result vector
        peer.getCounter(RowCounter.TOTAL_ROWS).increment(1L);
        int key = row.getKey().get();
        double sum = 0;
        SparseVectorWritable mRow = row.getValue();
        if (v.getSize() != mRow.getSize())
          throw new RuntimeException("Matrix row with index = " + key
              + " is not consistent with input vector. Row size = "
              + mRow.getSize() + " vector size = " + v.getSize());
        List<Integer> mIndeces = mRow.getIndeces();
        List<Double> mValues = mRow.getValues();
        for (int i = 0; i < mIndeces.size(); i++)
          sum += v.get(mIndeces.get(i)) * mValues.get(i);
        peer.write(new IntWritable(key), new DoubleWritable(sum));
      }
    }

  }

  /**
   * Method which actually starts SpMV.
   */
  private static void startTask(HamaConfiguration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    BSPJob bsp = new BSPJob(conf, SpMV.class);
    bsp.setJobName("Sparse matrix vector multiplication");
    bsp.setBspClass(SpMVBSP.class);
    /*
     * Input matrix is presented as pairs of integer and SparseVectorWritable.
     * Output is pairs of integer and double
     */
    bsp.setInputFormat(SequenceFileInputFormat.class);
    bsp.setInputKeyClass(IntWritable.class);
    bsp.setInputValueClass(SparseVectorWritable.class);
    bsp.setOutputKeyClass(IntWritable.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(SequenceFileOutputFormat.class);
    bsp.setInputPath(new Path(conf.get(inputMatrixPathString)));

    FileOutputFormat.setOutputPath(bsp, new Path(conf.get(outputPathString)));

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);

    int requestedTasks = conf.getInt(requestedBspTasksString, -1);
    if (requestedTasks != -1) {
      bsp.setNumBspTask(requestedTasks);
    } else {
      bsp.setNumBspTask(cluster.getMaxTasks());
    }

    long startTime = System.currentTimeMillis();
    if (bsp.waitForCompletion(true)) {
      LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime)
          / 1000.0 + " seconds.");
      convertToDenseVector(conf);
      LOG.info("Result is in " + getResultPath());
    } else {
      setResultPath(null);
    }
  }

  private static void printUsage() {
    LOG.info("Usage: spmv <Matrix> <Vector> <output> [number of tasks (default max)]");
  }

  /**
   * Function parses command line in standart form.
   */
  private static void parseArgs(HamaConfiguration conf, String[] args) {
    if (args.length < 3) {
      printUsage();
      System.exit(-1);
    }

    conf.set(inputMatrixPathString, args[0]);
    conf.set(inputVectorPathString, args[1]);

    Path path = new Path(args[2]);
    path = path.suffix(intermediate);
    conf.set(outputPathString, path.toString());

    if (args.length == 4) {
      try {
        int taskCount = Integer.parseInt(args[3]);
        if (taskCount < 0) {
          printUsage();
          throw new IllegalArgumentException(
              "The number of requested tasks can't be negative. Actual value: "
                  + String.valueOf(taskCount));
        }
        conf.setInt(requestedBspTasksString, taskCount);
      } catch (NumberFormatException e) {
        printUsage();
        throw new IllegalArgumentException(
            "The format of requested task count is int. Can not parse value: "
                + args[3]);
      }
    }
  }

  /**
   * SpMV produces a file, which contains result dense vector in format of pairs
   * of integer and double. The aim of this method is to convert SpMV output to
   * format usable in subsequent computation - dense vector. It can be usable
   * for iterative solvers. IMPORTANT: currently it is used in SpMV. It can be a
   * bottle neck, because all input needs to be stored in memory.
   * 
   * @param SpMVoutputPathString output path, which represents directory with
   *          part files.
   * @param conf configuration
   * @return path to output vector.
   * @throws IOException
   */
  public static String convertSpMVOutputToDenseVector(
      String SpMVoutputPathString, HamaConfiguration conf) throws IOException {
    List<Integer> indeces = new ArrayList<Integer>();
    List<Double> values = new ArrayList<Double>();

    FileSystem fs = FileSystem.get(conf);
    Path SpMVOutputPath = new Path(SpMVoutputPathString);
    Path resultOutputPath = SpMVOutputPath.getParent().suffix("/result");
    FileStatus[] stats = fs.listStatus(SpMVOutputPath);
    for (FileStatus stat : stats) {
      String filePath = stat.getPath().toUri().getPath();
      SequenceFile.Reader reader = null;
      fs.open(new Path(filePath));
      try {
        reader = new SequenceFile.Reader(fs, new Path(filePath), conf);
        IntWritable key = new IntWritable();
        DoubleWritable value = new DoubleWritable();
        while (reader.next(key, value)) {
          indeces.add(key.get());
          values.add(value.get());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        if (reader != null)
          reader.close();
      }
    }
    DenseVectorWritable result = new DenseVectorWritable();
    result.setSize(indeces.size());
    for (int i = 0; i < indeces.size(); i++)
      result.addCell(indeces.get(i), values.get(i));
    writeToFile(resultOutputPath.toString(), result, conf);
    return resultOutputPath.toString();
  }

  public static void readFromFile(String pathString, Writable result,
      HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Reader reader = null;
    Path path = new Path(pathString);
    List<String> filePaths = new ArrayList<String>();
    if (!fs.isFile(path)) {
      FileStatus[] stats = fs.listStatus(path);
      for (FileStatus stat : stats) {
        filePaths.add(stat.getPath().toUri().getPath());
      }
    } else if (fs.isFile(path)) {
      filePaths.add(path.toString());
    }

    try {
      for (String filePath : filePaths) {
        reader = new SequenceFile.Reader(fs, new Path(filePath), conf);
        IntWritable key = new IntWritable();
        reader.next(key, result);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * This method is used to write vector from memory to specified path.
   * 
   * @param pathString output path
   * @param result instance of vector to be writed
   * @param conf configuration
   * @throws IOException
   */
  public static void writeToFile(String pathString, Writable result,
      HamaConfiguration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    SequenceFile.Writer writer = null;
    try {
      writer = new SequenceFile.Writer(fs, conf, new Path(pathString),
          IntWritable.class, result.getClass());
      IntWritable key = new IntWritable();
      writer.append(key, result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (writer != null)
        writer.close();
    }
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    HamaConfiguration conf = new HamaConfiguration();
    parseArgs(conf, args);
    startTask(conf);
  }

}
