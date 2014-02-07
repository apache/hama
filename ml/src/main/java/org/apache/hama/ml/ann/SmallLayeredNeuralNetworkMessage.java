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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.commons.io.MatrixWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DoubleMatrix;

/**
 * NeuralNetworkMessage transmits the messages between peers during the training
 * of neural networks.
 * 
 */
public class SmallLayeredNeuralNetworkMessage implements Writable {

  protected double trainingError;
  protected DoubleMatrix[] curMatrices;
  protected DoubleMatrix[] prevMatrices;
  protected boolean converge;

  public SmallLayeredNeuralNetworkMessage() {
  }
  
  public SmallLayeredNeuralNetworkMessage(double trainingError,
      boolean converge, DoubleMatrix[] weightMatrices,
      DoubleMatrix[] prevMatrices) {
    this.trainingError = trainingError;
    this.converge = converge;
    this.curMatrices = weightMatrices;
    this.prevMatrices = prevMatrices;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    trainingError = input.readDouble();
    converge = input.readBoolean();
    int numMatrices = input.readInt();
    boolean hasPrevMatrices = input.readBoolean();
    curMatrices = new DenseDoubleMatrix[numMatrices];
    // read matrice updates
    for (int i = 0; i < curMatrices.length; ++i) {
      curMatrices[i] = (DenseDoubleMatrix) MatrixWritable.read(input);
    }

    if (hasPrevMatrices) {
      prevMatrices = new DenseDoubleMatrix[numMatrices];
      // read previous matrices updates
      for (int i = 0; i < prevMatrices.length; ++i) {
        prevMatrices[i] = (DenseDoubleMatrix) MatrixWritable.read(input);
      }
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeDouble(trainingError);
    output.writeBoolean(converge);
    output.writeInt(curMatrices.length);
    if (prevMatrices == null) {
      output.writeBoolean(false);
    } else {
      output.writeBoolean(true);
    }
    for (DoubleMatrix matrix : curMatrices) {
      MatrixWritable.write(matrix, output);
    }
    if (prevMatrices != null) {
      for (DoubleMatrix matrix : prevMatrices) {
        MatrixWritable.write(matrix, output);
      }
    }
  }

  public double getTrainingError() {
    return trainingError;
  }

  public void setTrainingError(double trainingError) {
    this.trainingError = trainingError;
  }

  public boolean isConverge() {
    return converge;
  }

  public void setConverge(boolean converge) {
    this.converge = converge;
  }

  public DoubleMatrix[] getCurMatrices() {
    return curMatrices;
  }

  public void setMatrices(DoubleMatrix[] curMatrices) {
    this.curMatrices = curMatrices;
  }

  public DoubleMatrix[] getPrevMatrices() {
    return prevMatrices;
  }

  public void setPrevMatrices(DoubleMatrix[] prevMatrices) {
    this.prevMatrices = prevMatrices;
  }

}
