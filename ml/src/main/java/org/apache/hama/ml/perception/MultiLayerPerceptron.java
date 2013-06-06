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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hama.ml.math.DoubleVector;

/**
 * PerceptronBase defines the common behavior of all the concrete perceptrons.
 */
public abstract class MultiLayerPerceptron {

  /* The trainer for the model */
  protected PerceptronTrainer trainer;
  /* The file path that contains the model meta-data */
  protected String modelPath;

  /* Model meta-data */
  protected String MLPType;
  protected double learningRate;
  protected boolean regularization;
  protected double momentum;
  protected int numberOfLayers;
  protected String squashingFunctionName;
  protected String costFunctionName;
  protected int[] layerSizeArray;

  protected CostFunction costFunction;
  protected SquashingFunction squashingFunction;

  /**
   * Initialize the MLP.
   * 
   * @param learningRate Larger learningRate makes MLP learn more aggressive.
   * @param regularization Turn on regularization make MLP less likely to
   *          overfit.
   * @param momentum The momentum makes the historical adjust have affect to
   *          current adjust.
   * @param squashingFunctionName The name of squashing function.
   * @param costFunctionName The name of the cost function.
   * @param layerSizeArray The number of neurons for each layer. Note that the
   *          actual size of each layer is one more than the input size.
   */
  public MultiLayerPerceptron(double learningRate, boolean regularization,
      double momentum, String squashingFunctionName, String costFunctionName,
      int[] layerSizeArray) {
    this.learningRate = learningRate;
    this.regularization = regularization; // no regularization
    this.momentum = momentum; // no momentum
    this.squashingFunctionName = squashingFunctionName;
    this.costFunctionName = costFunctionName;
    this.layerSizeArray = layerSizeArray;
    this.numberOfLayers = this.layerSizeArray.length;

    this.costFunction = CostFunctionFactory
        .getCostFunction(this.costFunctionName);
    this.squashingFunction = SquashingFunctionFactory
        .getSquashingFunction(this.squashingFunctionName);
  }

  /**
   * Initialize a multi-layer perceptron with existing model.
   * 
   * @param modelPath Location of existing model meta-data.
   */
  public MultiLayerPerceptron(String modelPath) {
    this.modelPath = modelPath;
  }

  /**
   * Train the model with given data. This method invokes a perceptron training
   * BSP task to train the model. It then write the model to modelPath.
   * 
   * @param dataInputPath The path of the data.
   * @param trainingParams Extra parameters for training.
   */
  public abstract void train(Path dataInputPath,
      Map<String, String> trainingParams) throws Exception;

  /**
   * Get the output based on the input instance and the learned model.
   * 
   * @param featureVector The feature of an instance to feed the perceptron.
   * @return The results.
   */
  public abstract DoubleVector output(DoubleVector featureVector)
      throws Exception;

  /**
   * Read the model meta-data from the specified location.
   * 
   * @throws IOException
   */
  protected abstract void readFromModel() throws IOException;

  /**
   * Write the model data to specified location.
   * 
   * @param modelPath The location in file system to store the model.
   * @throws IOException
   */
  public abstract void writeModelToFile(String modelPath) throws IOException;

  public String getModelPath() {
    return modelPath;
  }

  public String getMLPType() {
    return MLPType;
  }

  public double getLearningRate() {
    return learningRate;
  }

  public boolean isRegularization() {
    return regularization;
  }

  public double getMomentum() {
    return momentum;
  }

  public int getNumberOfLayers() {
    return numberOfLayers;
  }

  public String getSquashingFunctionName() {
    return squashingFunctionName;
  }

  public String getCostFunctionName() {
    return costFunctionName;
  }

  public int[] getLayerSizeArray() {
    return layerSizeArray;
  }

}
