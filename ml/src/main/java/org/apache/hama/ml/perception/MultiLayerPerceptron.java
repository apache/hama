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
import org.apache.hama.commons.math.DoubleDoubleFunction;
import org.apache.hama.commons.math.DoubleFunction;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.ml.ann.NeuralNetworkTrainer;
import org.apache.hama.ml.util.DefaultFeatureTransformer;
import org.apache.hama.ml.util.FeatureTransformer;

/**
 * PerceptronBase defines the common behavior of all the concrete perceptrons.
 */
public abstract class MultiLayerPerceptron {

  /* The trainer for the model */
  protected NeuralNetworkTrainer trainer;
  /* The file path that contains the model meta-data */
  protected String modelPath;

  /* Model meta-data */
  protected String MLPType;
  protected double learningRate;
  protected double regularization;
  protected double momentum;
  protected int numberOfLayers;
  protected String squashingFunctionName;
  protected String costFunctionName;
  protected int[] layerSizeArray;

  protected DoubleDoubleFunction costFunction;
  protected DoubleFunction squashingFunction;

  // transform the original features to new space
  protected FeatureTransformer featureTransformer;

  /**
   * Initialize the MLP.
   * 
   * @param learningRate Larger learningRate makes MLP learn more aggressive.
   *          Learning rate cannot be negative.
   * @param regularization Regularization makes MLP less likely to overfit. The
   *          value of regularization cannot be negative or too large, otherwise
   *          it will affect the precision.
   * @param momentum The momentum makes the historical adjust have affect to
   *          current adjust. The weight of momentum cannot be negative.
   * @param squashingFunctionName The name of squashing function.
   * @param costFunctionName The name of the cost function.
   * @param layerSizeArray The number of neurons for each layer. Note that the
   *          actual size of each layer is one more than the input size.
   */
  public MultiLayerPerceptron(double learningRate, double regularization,
      double momentum, String squashingFunctionName, String costFunctionName,
      int[] layerSizeArray) {
    this.MLPType = getTypeName();
    if (learningRate <= 0) {
      throw new IllegalStateException("learning rate cannot be negative.");
    }
    this.learningRate = learningRate;
    if (regularization < 0 || regularization >= 0.5) {
      throw new IllegalStateException(
          "regularization weight must be in range (0, 0.5).");
    }
    this.regularization = regularization; // no regularization
    if (momentum < 0) {
      throw new IllegalStateException("momentum weight cannot be negative.");
    }
    this.momentum = momentum; // no momentum
    this.squashingFunctionName = squashingFunctionName;
    this.costFunctionName = costFunctionName;
    this.layerSizeArray = layerSizeArray;
    this.numberOfLayers = this.layerSizeArray.length;

    this.costFunction = FunctionFactory
        .createDoubleDoubleFunction(this.costFunctionName);
    this.squashingFunction = FunctionFactory
        .createDoubleFunction(this.squashingFunctionName);

    this.featureTransformer = new DefaultFeatureTransformer();
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
  public DoubleVector output(DoubleVector featureVector) {
    return this.outputWrapper(this.featureTransformer.transform(featureVector));
  }

  public abstract DoubleVector outputWrapper(DoubleVector featureVector);

  /**
   * Use the class name as the type name.
   */
  protected abstract String getTypeName();

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

  public double isRegularization() {
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

  /**
   * Set the feature transformer.
   * 
   * @param featureTransformer
   */
  public void setFeatureTransformer(FeatureTransformer featureTransformer) {
    this.featureTransformer = featureTransformer;
  }
  
  public FeatureTransformer getFeatureTransformer() {
    return this.featureTransformer;
  }

}
