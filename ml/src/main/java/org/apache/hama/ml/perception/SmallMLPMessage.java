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

import org.apache.hama.ml.math.DenseDoubleMatrix;
import org.apache.hama.ml.writable.MatrixWritable;

/**
 * SmallMLPMessage is used to exchange information for the
 * {@link SmallMultiLayerPerceptron}. It send the whole parameter matrix from
 * one task to another.
 */
public class SmallMLPMessage extends MLPMessage {

  private int owner; // the ID of the task who creates the message
  private DenseDoubleMatrix[] weightUpdatedMatrices;
  private int numOfMatrices;

  public SmallMLPMessage(int owner, boolean terminated, DenseDoubleMatrix[] mat) {
    super(terminated);
    this.owner = owner;
    this.weightUpdatedMatrices = mat;
    this.numOfMatrices = this.weightUpdatedMatrices == null ? 0
        : this.weightUpdatedMatrices.length;
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
  public DenseDoubleMatrix[] getWeightsUpdatedMatrices() {
    return this.weightUpdatedMatrices;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.owner = input.readInt();
    this.terminated = input.readBoolean();
    this.numOfMatrices = input.readInt();
    this.weightUpdatedMatrices = new DenseDoubleMatrix[this.numOfMatrices];
    for (int i = 0; i < this.numOfMatrices; ++i) {
      this.weightUpdatedMatrices[i] = (DenseDoubleMatrix) MatrixWritable
          .read(input);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(this.owner);
    output.writeBoolean(this.terminated);
    output.writeInt(this.numOfMatrices);
    for (int i = 0; i < this.numOfMatrices; ++i) {
      MatrixWritable.write(this.weightUpdatedMatrices[i], output);
    }
  }

}
