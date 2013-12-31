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
import java.util.List;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.commons.math.DoubleDoubleFunction;
import org.apache.hama.commons.math.DoubleFunction;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * AbstractLayeredNeuralNetwork defines the general operations for derivative
 * layered models, include Linear Regression, Logistic Regression, Multilayer
 * Perceptron, Autoencoder, and Restricted Boltzmann Machine, etc.
 * 
 * In general, these models consist of neurons which are aligned in layers.
 * Between layers, for any two adjacent layers, the neurons are connected to
 * form a bipartite weighted graph.
 * 
 */
abstract class AbstractLayeredNeuralNetwork extends NeuralNetwork {

  private static final double DEFAULT_REGULARIZATION_WEIGHT = 0;
  private static final double DEFAULT_MOMENTUM_WEIGHT = 0.1;

  double trainingError;

  /* The weight of regularization */
  protected double regularizationWeight;

  /* The momentumWeight */
  protected double momentumWeight;

  /* The cost function of the model */
  protected DoubleDoubleFunction costFunction;

  /* Record the size of each layer */
  protected List<Integer> layerSizeList;

  protected TrainingMethod trainingMethod;
  
  protected LearningStyle learningStyle;

  public static enum TrainingMethod {
    GRADIENT_DESCENT
  }
  
  public static enum LearningStyle {
    UNSUPERVISED,
    SUPERVISED
  }
  
  public AbstractLayeredNeuralNetwork() {
    this.regularizationWeight = DEFAULT_REGULARIZATION_WEIGHT;
    this.momentumWeight = DEFAULT_MOMENTUM_WEIGHT;
    this.trainingMethod = TrainingMethod.GRADIENT_DESCENT;
    this.learningStyle = LearningStyle.SUPERVISED;
  }

  public AbstractLayeredNeuralNetwork(String modelPath) {
    super(modelPath);
  }

  /**
   * Set the regularization weight. Recommend in the range [0, 0.1), More
   * complex the model is, less weight the regularization is.
   * 
   * @param regularization
   */
  public void setRegularizationWeight(double regularizationWeight) {
    Preconditions.checkArgument(regularizationWeight >= 0
        && regularizationWeight < 1.0,
        "Regularization weight must be in range [0, 1.0)");
    this.regularizationWeight = regularizationWeight;
  }

  public double getRegularizationWeight() {
    return this.regularizationWeight;
  }

  /**
   * Set the momemtum weight for the model. Recommend in range [0, 0.5].
   * 
   * @param momentumWeight
   */
  public void setMomemtumWeight(double momentumWeight) {
    Preconditions.checkArgument(momentumWeight >= 0 && momentumWeight <= 1.0,
        "Momentum weight must be in range [0, 1.0]");
    this.momentumWeight = momentumWeight;
  }

  public double getMomemtumWeight() {
    return this.momentumWeight;
  }

  public void setTrainingMethod(TrainingMethod method) {
    this.trainingMethod = method;
  }

  public TrainingMethod getTrainingMethod() {
    return this.trainingMethod;
  }
  
  public void setLearningStyle(LearningStyle style) {
    this.learningStyle = style;
  }
  
  public LearningStyle getLearningStyle() {
    return this.learningStyle;
  }

  /**
   * Set the cost function for the model.
   * 
   * @param costFunctionName
   */
  public void setCostFunction(DoubleDoubleFunction costFunction) {
    this.costFunction = costFunction;
  }

  /**
   * Add a layer of neurons with specified size. If the added layer is not the
   * first layer, it will automatically connects the neurons between with the
   * previous layer.
   * 
   * @param size
   * @param isFinalLayer If false, add a bias neuron.
   * @param squashingFunction The squashing function for this layer, input layer
   *          is f(x) = x by default.
   * @return The layer index, starts with 0.
   */
  public abstract int addLayer(int size, boolean isFinalLayer,
      DoubleFunction squashingFunction);

  /**
   * Get the size of a particular layer.
   * 
   * @param layer
   * @return
   */
  public int getLayerSize(int layer) {
    Preconditions.checkArgument(
        layer >= 0 && layer < this.layerSizeList.size(),
        String.format("Input must be in range [0, %d]\n",
            this.layerSizeList.size() - 1));
    return this.layerSizeList.get(layer);
  }

  /**
   * Get the layer size list.
   * 
   * @return
   */
  protected List<Integer> getLayerSizeList() {
    return this.layerSizeList;
  }

  /**
   * Get the weights between layer layerIdx and layerIdx + 1
   * 
   * @param layerIdx The index of the layer
   * @return The weights in form of {@link DoubleMatrix}
   */
  public abstract DoubleMatrix getWeightsByLayer(int layerIdx);

  /**
   * Get the updated weights using one training instance.
   * 
   * @param trainingInstance The trainingInstance is the concatenation of
   *          feature vector and class label vector.
   * @return The update of each weight, in form of matrix list.
   * @throws Exception
   */
  public abstract DoubleMatrix[] trainByInstance(DoubleVector trainingInstance);

  /**
   * Get the output calculated by the model.
   * 
   * @param instance The feature instance.
   * @return
   */
  public abstract DoubleVector getOutput(DoubleVector instance);

  /**
   * Calculate the training error based on the labels and outputs.
   * 
   * @param labels
   * @param output
   * @return
   */
  protected abstract void calculateTrainingError(DoubleVector labels,
      DoubleVector output);

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    // read regularization weight
    this.regularizationWeight = input.readDouble();
    // read momentum weight
    this.momentumWeight = input.readDouble();

    // read cost function
    this.costFunction = FunctionFactory
        .createDoubleDoubleFunction(WritableUtils.readString(input));

    // read layer size list
    int numLayers = input.readInt();
    this.layerSizeList = Lists.newArrayList();
    for (int i = 0; i < numLayers; ++i) {
      this.layerSizeList.add(input.readInt());
    }

    this.trainingMethod = WritableUtils.readEnum(input, TrainingMethod.class);
    this.learningStyle = WritableUtils.readEnum(input, LearningStyle.class);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    // write regularization weight
    output.writeDouble(this.regularizationWeight);
    // write momentum weight
    output.writeDouble(this.momentumWeight);

    // write cost function
    WritableUtils.writeString(output, costFunction.getFunctionName());

    // write layer size list
    output.writeInt(this.layerSizeList.size());
    for (Integer aLayerSizeList : this.layerSizeList) {
      output.writeInt(aLayerSizeList);
    }

    WritableUtils.writeEnum(output, this.trainingMethod);
    WritableUtils.writeEnum(output, this.learningStyle);
  }

}
