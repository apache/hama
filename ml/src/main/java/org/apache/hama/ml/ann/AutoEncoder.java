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

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleFunction;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.ml.ann.AbstractLayeredNeuralNetwork.LearningStyle;
import org.apache.hama.ml.util.FeatureTransformer;

import com.google.common.base.Preconditions;

/**
 * AutoEncoder is a model used for dimensional reduction and feature learning.
 * It is a special kind of {@link NeuralNetwork} that consists of three layers
 * of neurons, where the first layer and third layer contains the same number of
 * neurons.
 * 
 */
public class AutoEncoder {

  private final SmallLayeredNeuralNetwork model;

  /**
   * Initialize the autoencoder.
   * 
   * @param inputDimensions The number of dimensions for the input feature.
   * @param compressedDimensions The number of dimensions for the compressed
   *          information.
   */
  public AutoEncoder(int inputDimensions, int compressedDimensions) {
    model = new SmallLayeredNeuralNetwork();
    model.addLayer(inputDimensions, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    model.addLayer(compressedDimensions, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    model.addLayer(inputDimensions, true,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    model.setLearningStyle(LearningStyle.UNSUPERVISED);
    model.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
  }

  public AutoEncoder(String modelPath) {
    model = new SmallLayeredNeuralNetwork(modelPath);
  }

  public AutoEncoder setLearningRate(double learningRate) {
    model.setLearningRate(learningRate);
    return this;
  }

  public AutoEncoder setMomemtumWeight(double momentumWeight) {
    model.setMomemtumWeight(momentumWeight);
    return this;
  }

  public AutoEncoder setRegularizationWeight(double regularizationWeight) {
    model.setRegularizationWeight(regularizationWeight);
    return this;
  }
  
  public AutoEncoder setModelPath(String modelPath) {
    model.setModelPath(modelPath);
    return this;
  }

  /**
   * Train the autoencoder with given data. Note that the training data is
   * pre-processed, where the features
   * 
   * @param dataInputPath
   * @param trainingParams
   */
  public void train(Path dataInputPath, Map<String, String> trainingParams) {
    model.train(dataInputPath, trainingParams);
  }

  /**
   * Train the model with one instance.
   * 
   * @param trainingInstance
   */
  public void trainOnline(DoubleVector trainingInstance) {
    model.trainOnline(trainingInstance);
  }

  /**
   * Get the matrix M used to encode the input features.
   * 
   * @return
   */
  public DoubleMatrix getEncodeWeightMatrix() {
    return model.getWeightsByLayer(0);
  }

  /**
   * Get the matrix M used to decode the compressed information.
   * 
   * @return
   */
  public DoubleMatrix getDecodeWeightMatrix() {
    return model.getWeightsByLayer(1);
  }

  /**
   * Transform the input features.
   * 
   * @param inputInstance
   * @return The compressed information.
   */
  private DoubleVector transform(DoubleVector inputInstance, int inputLayer) {
    DoubleVector internalInstance = new DenseDoubleVector(inputInstance.getDimension() + 1);
    internalInstance.set(0, 1);
    for (int i = 0; i < inputInstance.getDimension(); ++i) {
      internalInstance.set(i + 1, inputInstance.get(i));
    }
    DoubleFunction squashingFunction = model
        .getSquashingFunction(inputLayer);
    DoubleMatrix weightMatrix = null;
    if (inputLayer == 0) {
      weightMatrix = this.getEncodeWeightMatrix();
    } else {
      weightMatrix = this.getDecodeWeightMatrix();
    }
    DoubleVector vec = weightMatrix.multiplyVectorUnsafe(internalInstance);
    vec = vec.applyToElements(squashingFunction);
    return vec;
  }

  /**
   * Encode the input instance.
   * @param inputInstance
   * @return
   */
  public DoubleVector encode(DoubleVector inputInstance) {
    Preconditions
        .checkArgument(
            inputInstance.getDimension() == model.getLayerSize(0) - 1,
            String.format("The dimension of input instance is %d, but the model requires dimension %d.",
                    inputInstance.getDimension(), model.getLayerSize(1) - 1));
    return this.transform(inputInstance, 0);
  }

  /**
   * Decode the input instance.
   * @param inputInstance
   * @return
   */
  public DoubleVector decode(DoubleVector inputInstance) {
    Preconditions
        .checkArgument(
            inputInstance.getDimension() == model.getLayerSize(1) - 1,
            String.format("The dimension of input instance is %d, but the model requires dimension %d.",
                    inputInstance.getDimension(), model.getLayerSize(1) - 1));
    return this.transform(inputInstance, 1);
  }
  
  /**
   * Get the label(s) according to the given features.
   * @param inputInstance
   * @return
   */
  public DoubleVector getOutput(DoubleVector inputInstance) {
    return model.getOutput(inputInstance);
  }
  
  /**
   * Set the feature transformer.
   * @param featureTransformer
   */
  public void setFeatureTransformer(FeatureTransformer featureTransformer) {
    this.model.setFeatureTransformer(featureTransformer);
  }

}
