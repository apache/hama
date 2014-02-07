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
package org.apache.hama.ml.perception;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.commons.io.MatrixWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;

/**
 * SmallMLPMessage is used to exchange information for the
 * {@link SmallMultiLayerPerceptron}. It send the whole parameter matrix from
 * one task to another.
 */
public class SmallMLPMessage extends MLPMessage {

  private int owner; // the ID of the task who creates the message
  private int numOfUpdatedMatrices;
  private DenseDoubleMatrix[] weightUpdatedMatrices;
  private int numOfPrevUpdatedMatrices;
  private DenseDoubleMatrix[] prevWeightUpdatedMatrices;

  public SmallMLPMessage() {
    super();
  }
  
  /**
   * When slave send message to master, use this constructor.
   * 
   * @param owner The owner that create the message
   * @param terminated Whether the training is terminated for the owner task
   * @param weightUpdatedMatrics The weight updates
   */
  public SmallMLPMessage(int owner, boolean terminated,
      DenseDoubleMatrix[] weightUpdatedMatrics) {
    super(terminated);
    this.owner = owner;
    this.weightUpdatedMatrices = weightUpdatedMatrics;
    this.numOfUpdatedMatrices = this.weightUpdatedMatrices == null ? 0
        : this.weightUpdatedMatrices.length;
    this.numOfPrevUpdatedMatrices = 0;
    this.prevWeightUpdatedMatrices = null;
  }

  /**
   * When master send message to slave, use this constructor.
   * 
   * @param owner The owner that create the message
   * @param terminated Whether the training is terminated for the owner task
   * @param weightUpdatedMatrics The weight updates
   * @param prevWeightUpdatedMatrices
   */
  public SmallMLPMessage(int owner, boolean terminated,
      DenseDoubleMatrix[] weightUpdatedMatrices,
      DenseDoubleMatrix[] prevWeightUpdatedMatrices) {
    this(owner, terminated, weightUpdatedMatrices);
    this.prevWeightUpdatedMatrices = prevWeightUpdatedMatrices;
    this.numOfPrevUpdatedMatrices = this.prevWeightUpdatedMatrices == null ? 0
        : this.prevWeightUpdatedMatrices.length;
  }

  /**
   * Get the owner task Id of the message.
   * 
   * @return
   */
  public int getOwner() {
    return owner;
  }

  /**
   * Get the updated weight matrices.
   * 
   * @return
   */
  public DenseDoubleMatrix[] getWeightUpdatedMatrices() {
    return this.weightUpdatedMatrices;
  }

  public DenseDoubleMatrix[] getPrevWeightsUpdatedMatrices() {
    return this.prevWeightUpdatedMatrices;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.owner = input.readInt();
    this.terminated = input.readBoolean();
    this.numOfUpdatedMatrices = input.readInt();
    this.weightUpdatedMatrices = new DenseDoubleMatrix[this.numOfUpdatedMatrices];
    for (int i = 0; i < this.numOfUpdatedMatrices; ++i) {
      this.weightUpdatedMatrices[i] = (DenseDoubleMatrix) MatrixWritable
          .read(input);
    }
    this.numOfPrevUpdatedMatrices = input.readInt();
    this.prevWeightUpdatedMatrices = new DenseDoubleMatrix[this.numOfPrevUpdatedMatrices];
    for (int i = 0; i < this.numOfPrevUpdatedMatrices; ++i) {
      this.prevWeightUpdatedMatrices[i] = (DenseDoubleMatrix) MatrixWritable
          .read(input);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(this.owner);
    output.writeBoolean(this.terminated);
    output.writeInt(this.numOfUpdatedMatrices);
    for (int i = 0; i < this.numOfUpdatedMatrices; ++i) {
      MatrixWritable.write(this.weightUpdatedMatrices[i], output);
    }
    output.writeInt(this.numOfPrevUpdatedMatrices);
    for (int i = 0; i < this.numOfPrevUpdatedMatrices; ++i) {
      MatrixWritable.write(this.prevWeightUpdatedMatrices[i], output);
    }
  }

}
