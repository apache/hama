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
package org.apache.hama.ml.ann;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DoubleMatrix;
import org.junit.Test;

/**
 * Test the functionalities of SmallLayeredNeuralNetworkMessage.
 * 
 */
public class TestSmallLayeredNeuralNetworkMessage {

  @Test
  public void testReadWriteWithoutPrev() {
    double error = 0.22;
    double[][] matrix1 = new double[][] { { 0.1, 0.2, 0.8, 0.5 },
        { 0.3, 0.4, 0.6, 0.2 }, { 0.5, 0.6, 0.1, 0.5 } };
    double[][] matrix2 = new double[][] { { 0.8, 1.2, 0.5 } };
    DoubleMatrix[] matrices = new DoubleMatrix[2];
    matrices[0] = new DenseDoubleMatrix(matrix1);
    matrices[1] = new DenseDoubleMatrix(matrix2);

    boolean isConverge = false;

    SmallLayeredNeuralNetworkMessage message = new SmallLayeredNeuralNetworkMessage(
        error, isConverge, matrices, null);
    Configuration conf = new Configuration();
    String strPath = "/tmp/testReadWriteSmallLayeredNeuralNetworkMessage";
    Path path = new Path(strPath);
    try {
      FileSystem fs = FileSystem.get(new URI(strPath), conf);
      FSDataOutputStream out = fs.create(path);
      message.write(out);
      out.close();

      FSDataInputStream in = fs.open(path);
      SmallLayeredNeuralNetworkMessage readMessage = new SmallLayeredNeuralNetworkMessage(
          0, isConverge, null, null);
      readMessage.readFields(in);
      in.close();
      assertEquals(error, readMessage.getTrainingError(), 0.000001);
      assertFalse(readMessage.isConverge());
      DoubleMatrix[] readMatrices = readMessage.getCurMatrices();
      assertEquals(2, readMatrices.length);
      for (int i = 0; i < readMatrices.length; ++i) {
        double[][] doubleMatrices = ((DenseDoubleMatrix) readMatrices[i])
            .getValues();
        double[][] doubleExpected = ((DenseDoubleMatrix) matrices[i])
            .getValues();
        for (int r = 0; r < doubleMatrices.length; ++r) {
          assertArrayEquals(doubleExpected[r], doubleMatrices[r], 0.000001);
        }
      }

      DoubleMatrix[] readPrevMatrices = readMessage.getPrevMatrices();
      assertNull(readPrevMatrices);

      // delete
      fs.delete(path, true);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testReadWriteWithPrev() {
    double error = 0.22;
    boolean isConverge = true;

    double[][] matrix1 = new double[][] { { 0.1, 0.2, 0.8, 0.5 },
        { 0.3, 0.4, 0.6, 0.2 }, { 0.5, 0.6, 0.1, 0.5 } };
    double[][] matrix2 = new double[][] { { 0.8, 1.2, 0.5 } };
    DoubleMatrix[] matrices = new DoubleMatrix[2];
    matrices[0] = new DenseDoubleMatrix(matrix1);
    matrices[1] = new DenseDoubleMatrix(matrix2);

    double[][] prevMatrix1 = new double[][] { { 0.1, 0.1, 0.2, 0.3 },
        { 0.2, 0.4, 0.1, 0.5 }, { 0.5, 0.1, 0.5, 0.2 } };
    double[][] prevMatrix2 = new double[][] { { 0.1, 0.2, 0.5, 0.9 },
        { 0.3, 0.5, 0.2, 0.6 }, { 0.6, 0.8, 0.7, 0.5 } };

    DoubleMatrix[] prevMatrices = new DoubleMatrix[2];
    prevMatrices[0] = new DenseDoubleMatrix(prevMatrix1);
    prevMatrices[1] = new DenseDoubleMatrix(prevMatrix2);

    SmallLayeredNeuralNetworkMessage message = new SmallLayeredNeuralNetworkMessage(
        error, isConverge, matrices, prevMatrices);
    Configuration conf = new Configuration();
    String strPath = "/tmp/testReadWriteSmallLayeredNeuralNetworkMessageWithPrev";
    Path path = new Path(strPath);
    try {
      FileSystem fs = FileSystem.get(new URI(strPath), conf);
      FSDataOutputStream out = fs.create(path);
      message.write(out);
      out.close();

      FSDataInputStream in = fs.open(path);
      SmallLayeredNeuralNetworkMessage readMessage = new SmallLayeredNeuralNetworkMessage(
          0, isConverge, null, null);
      readMessage.readFields(in);
      in.close();

      assertTrue(readMessage.isConverge());

      DoubleMatrix[] readMatrices = readMessage.getCurMatrices();
      assertEquals(2, readMatrices.length);
      for (int i = 0; i < readMatrices.length; ++i) {
        double[][] doubleMatrices = ((DenseDoubleMatrix) readMatrices[i])
            .getValues();
        double[][] doubleExpected = ((DenseDoubleMatrix) matrices[i])
            .getValues();
        for (int r = 0; r < doubleMatrices.length; ++r) {
          assertArrayEquals(doubleExpected[r], doubleMatrices[r], 0.000001);
        }
      }

      DoubleMatrix[] readPrevMatrices = readMessage.getPrevMatrices();
      assertEquals(2, readPrevMatrices.length);
      for (int i = 0; i < readPrevMatrices.length; ++i) {
        double[][] doubleMatrices = ((DenseDoubleMatrix) readPrevMatrices[i])
            .getValues();
        double[][] doubleExpected = ((DenseDoubleMatrix) prevMatrices[i])
            .getValues();
        for (int r = 0; r < doubleMatrices.length; ++r) {
          assertArrayEquals(doubleExpected[r], doubleMatrices[r], 0.000001);
        }
      }

      // delete
      fs.delete(path, true);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

}
