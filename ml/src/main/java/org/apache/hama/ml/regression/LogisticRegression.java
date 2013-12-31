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

package org.apache.hama.ml.regression;

import org.apache.hadoop.fs.Path;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.ml.ann.SmallLayeredNeuralNetwork;
import org.apache.hama.ml.util.FeatureTransformer;

import java.io.IOException;
import java.util.Map;

/**
 * The logistic regression model. It can be used to conduct 2-class
 * classification.
 * 
 */
public class LogisticRegression {
  
  private final SmallLayeredNeuralNetwork ann;
  
  public LogisticRegression(int dimension) {
    this.ann = new SmallLayeredNeuralNetwork();
    this.ann.addLayer(dimension, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    this.ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    this.ann.setCostFunction(FunctionFactory.createDoubleDoubleFunction("CrossEntropy"));
  }
  
  public LogisticRegression(String modelPath) {
    this.ann = new SmallLayeredNeuralNetwork(modelPath);
  }
  
  /**
   * Set the learning rate, recommend in range (0, 0.01]. Note that linear
   * regression are easy to get diverge if the learning rate is not small
   * enough.
   * 
   * @param learningRate
   */
  public LogisticRegression setLearningRate(double learningRate) {
    ann.setLearningRate(learningRate);
    return this;
  }

  /**
   * Get the learning rate.
   */
  public double getLearningRate() {
    return ann.getLearningRate();
  }

  /**
   * Set the weight of the momemtum. Recommend in range [0, 1.0]. Too large
   * momemtum weight may make model hard to converge.
   * 
   * @param momemtumWeight
   */
  public LogisticRegression setMomemtumWeight(double momemtumWeight) {
    ann.setMomemtumWeight(momemtumWeight);
    return this;
  }

  /**
   * Get the weight of momemtum.
   * 
   * @return
   */
  public double getMomemtumWeight() {
    return ann.getMomemtumWeight();
  }

  /**
   * Set the weight of regularization, recommend in range [0, 0.1]. Too large
   * regularization will mislead the model.
   * 
   * @param regularizationWeight
   */
  public LogisticRegression setRegularizationWeight(double regularizationWeight) {
    ann.setRegularizationWeight(regularizationWeight);
    return this;
  }

  /**
   * Get the weight of regularization.
   * 
   * @return
   */
  public double getRegularizationWeight() {
    return ann.getRegularizationWeight();
  }

  /**
   * Train the linear regression model with one instance. It is HIGHLY
   * RECOMMENDED to normalize the data first.
   * 
   * @param trainingInstance
   */
  public void trainOnline(DoubleVector trainingInstance) {
    ann.trainOnline(trainingInstance);
  }

  /**
   * Train the model with given data. It is HIGHLY RECOMMENDED to normalize the
   * data first.
   * 
   * @param dataInputPath The file path that contains the training instance.
   * @param trainingParams The training parameters.
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public void train(Path dataInputPath, Map<String, String> trainingParams) {
    ann.train(dataInputPath, trainingParams);
  }

  /**
   * Get the output according to given input instance.
   * 
   * @param instance
   * @return
   */
  public DoubleVector getOutput(DoubleVector instance) {
    return ann.getOutput(instance);
  }

  /**
   * Set the path to store the model. Note this is just set the path, it does
   * not save the model. You should call writeModelToFile to save the model.
   * 
   * @param modelPath
   */
  public void setModelPath(String modelPath) {
    ann.setModelPath(modelPath);
  }

  /**
   * Save the model to specified model path.
   */
  public void writeModelToFile() {
    try {
      ann.writeModelToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Get the weights of the model.
   * 
   * @return
   */
  public DoubleVector getWeights() {
    return ann.getWeightsByLayer(0).getRowVector(0);
  }

  /**
   * Set the feature transformer.
   * @param featureTransformer
   */
  public void setFeatureTransformer(FeatureTransformer featureTransformer) {
    this.ann.setFeatureTransformer(featureTransformer);
  }
}
