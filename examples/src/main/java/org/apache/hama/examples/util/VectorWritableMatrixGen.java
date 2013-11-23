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
package org.apache.hama.examples.util;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.commons.io.PipesVectorWritable;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleVector;

public class VectorWritableMatrixGen {
  private static final Log LOG = LogFactory
      .getLog(VectorWritableMatrixGen.class);

  public static void main(String[] args) throws InterruptedException,
      IOException, ClassNotFoundException {

    boolean saveTransposed = false;
    boolean usePipesVectorWritable = false;
    double minValue = 0;
    double maxValue = 1000;
    int precision = 3;

    // check arguments
    if (args.length < 3) {
      System.out
          .println("Usage: <rowSize> <colSize> <outputPath>"
              + " [<saveTransposed=true|false(default)>] [<usePipesVectorWritable=true|false(default)>]"
              + " [<minValue=" + minValue + ">] [<maxValue=" + maxValue + ">]"
              + " [<precision=" + precision + ">]");
      System.out
          .println("e.g., hama jar hama-examples-*.jar gen vectorwritablematrix 10 10 /tmp/matrix.seq");
      System.out
          .println("      hama jar hama-examples-*.jar gen vectorwritablematrix 10 10 /tmp/matrix.seq"
              + " false false 0 10 2");
      System.exit(1);
    }

    int rowSize = Integer.parseInt(args[0]);
    int colSize = Integer.parseInt(args[1]);
    Path outputPath = new Path(args[2]);

    if (args.length > 3) {
      saveTransposed = Boolean.parseBoolean(args[3]);
      if (args.length > 4) {
        usePipesVectorWritable = Boolean.parseBoolean(args[4]);
        if (args.length > 5) {
          minValue = Double.parseDouble(args[5]);
          if (args.length > 6) {
            maxValue = Double.parseDouble(args[6]);
            if (args.length > 7) {
              precision = Integer.parseInt(args[7]);
            }
          }
        }
      }
    }

    LOG.debug("rowSize: " + rowSize + " colSize: " + colSize + " outputPath: "
        + outputPath);
    LOG.debug("saveTransposed: " + saveTransposed + " usePipesVectorWritable: "
        + usePipesVectorWritable);
    LOG.debug("minValue: " + minValue + " maxValue: " + maxValue
        + " precision: " + precision);

    // create random double matrix
    double[][] matrix = createRandomMatrix(rowSize, colSize, new Random(),
        minValue, maxValue, precision);

    // write matrix to dfs
    writeMatrix(matrix, outputPath, saveTransposed, usePipesVectorWritable);
  }

  public static double[][] createRandomMatrix(int rows, int columns,
      Random rand, double rangeMin, double rangeMax, int precision) {

    LOG.debug("createRandomMatrix rows: " + rows + " cols: " + columns);

    final double[][] matrix = new double[rows][columns];

    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < columns; j++) {
        double randomValue = rangeMin + (rangeMax - rangeMin)
            * rand.nextDouble();

        matrix[i][j] = new BigDecimal(randomValue).setScale(precision,
            BigDecimal.ROUND_DOWN).doubleValue();
      }
    }
    return matrix;
  }

  public static Path writeMatrix(double[][] matrix, Path path,
      boolean saveTransposed, boolean usePipesVectorWritable) {

    LOG.debug("writeMatrix path: " + path + " saveTransposed: "
        + saveTransposed + " usePipesVectorWritable: " + usePipesVectorWritable);

    // transpose matrix before saving
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

    // Write matrix to DFS
    HamaConfiguration conf = new HamaConfiguration();
    SequenceFile.Writer writer = null;
    try {
      FileSystem fs = FileSystem.get(conf);
      // use PipesVectorWritable if specified
      if (usePipesVectorWritable) {
        writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class,
            PipesVectorWritable.class);

        for (int i = 0; i < matrix.length; i++) {
          DenseDoubleVector rowVector = new DenseDoubleVector(matrix[i]);
          writer.append(new IntWritable(i), new PipesVectorWritable(rowVector));
          LOG.debug("IntWritable: " + i + " PipesVectorWritable: "
              + rowVector.toString());
        }

      } else {
        writer = new SequenceFile.Writer(fs, conf, path, IntWritable.class,
            VectorWritable.class);

        for (int i = 0; i < matrix.length; i++) {
          DenseDoubleVector rowVector = new DenseDoubleVector(matrix[i]);
          writer.append(new IntWritable(i), new VectorWritable(rowVector));
          LOG.debug("IntWritable: " + i + " VectorWritable: "
              + rowVector.toString());
        }
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

}
