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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.commons.io.MatrixWritable;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleFunction;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.mortbay.log.Log;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * SmallLayeredNeuralNetwork defines the general operations for derivative
 * layered models, include Linear Regression, Logistic Regression, Multilayer
 * Perceptron, Autoencoder, and Restricted Boltzmann Machine, etc. For
 * SmallLayeredNeuralNetwork, the training can be conducted in parallel, but the
 * parameters of the models are assumes to be stored in a single machine.
 * 
 * In general, these models consist of neurons which are aligned in layers.
 * Between layers, for any two adjacent layers, the neurons are connected to
 * form a bipartite weighted graph.
 * 
 */
public class SmallLayeredNeuralNetwork extends AbstractLayeredNeuralNetwork {

  /* Weights between neurons at adjacent layers */
  protected List<DoubleMatrix> weightMatrixList;

  /* Previous weight updates between neurons at adjacent layers */
  protected List<DoubleMatrix> prevWeightUpdatesList;

  /* Different layers can have different squashing function */
  protected List<DoubleFunction> squashingFunctionList;

  protected int finalLayerIdx;

  public SmallLayeredNeuralNetwork() {
    this.layerSizeList = Lists.newArrayList();
    this.weightMatrixList = Lists.newArrayList();
    this.prevWeightUpdatesList = Lists.newArrayList();
    this.squashingFunctionList = Lists.newArrayList();
  }

  public SmallLayeredNeuralNetwork(String modelPath) {
    super(modelPath);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public int addLayer(int size, boolean isFinalLayer,
      DoubleFunction squashingFunction) {
    Preconditions.checkArgument(size > 0,
        "Size of layer must be larger than 0.");
    if (!isFinalLayer) {
      size += 1;
    }

    this.layerSizeList.add(size);
    int layerIdx = this.layerSizeList.size() - 1;
    if (isFinalLayer) {
      this.finalLayerIdx = layerIdx;
    }

    // add weights between current layer and previous layer, and input layer has
    // no squashing function
    if (layerIdx > 0) {
      int sizePrevLayer = this.layerSizeList.get(layerIdx - 1);
      // row count equals to size of current size and column count equals to
      // size of previous layer
      int row = isFinalLayer ? size : size - 1;
      int col = sizePrevLayer;
      DoubleMatrix weightMatrix = new DenseDoubleMatrix(row, col);
      // initialize weights
      weightMatrix.applyToElements(new DoubleFunction() {
        @Override
        public double apply(double value) {
          return RandomUtils.nextDouble() - 0.5;
        }

        @Override
        public double applyDerivative(double value) {
          throw new UnsupportedOperationException("");
        }
      });
      this.weightMatrixList.add(weightMatrix);
      this.prevWeightUpdatesList.add(new DenseDoubleMatrix(row, col));
      this.squashingFunctionList.add(squashingFunction);
    }
    return layerIdx;
  }

  /**
   * Update the weight matrices with given matrices.
   * 
   * @param matrices
   */
  public void updateWeightMatrices(DoubleMatrix[] matrices) {
    for (int i = 0; i < matrices.length; ++i) {
      DoubleMatrix matrix = this.weightMatrixList.get(i);
      this.weightMatrixList.set(i, matrix.add(matrices[i]));
    }
  }

  /**
   * Set the previous weight matrices.
   * @param prevUpdates
   */
  void setPrevWeightMatrices(DoubleMatrix[] prevUpdates) {
    this.prevWeightUpdatesList.clear();
    Collections.addAll(this.prevWeightUpdatesList, prevUpdates);
  }

  /**
   * Add a batch of matrices onto the given destination matrices.
   * 
   * @param destMatrices
   * @param sourceMatrices
   */
  static void matricesAdd(DoubleMatrix[] destMatrices,
      DoubleMatrix[] sourceMatrices) {
    for (int i = 0; i < destMatrices.length; ++i) {
      destMatrices[i] = destMatrices[i].add(sourceMatrices[i]);
    }
  }

  /**
   * Get all the weight matrices.
   * 
   * @return
   */
  DoubleMatrix[] getWeightMatrices() {
    DoubleMatrix[] matrices = new DoubleMatrix[this.weightMatrixList.size()];
    this.weightMatrixList.toArray(matrices);
    return matrices;
  }

  /**
   * Set the weight matrices.
   * 
   * @param matrices
   */
  public void setWeightMatrices(DoubleMatrix[] matrices) {
    this.weightMatrixList = new ArrayList<DoubleMatrix>();
    Collections.addAll(this.weightMatrixList, matrices);
  }

  /**
   * Get the previous matrices updates in form of array.
   * 
   * @return
   */
  public DoubleMatrix[] getPrevMatricesUpdates() {
    DoubleMatrix[] prevMatricesUpdates = new DoubleMatrix[this.prevWeightUpdatesList
        .size()];
    for (int i = 0; i < this.prevWeightUpdatesList.size(); ++i) {
      prevMatricesUpdates[i] = this.prevWeightUpdatesList.get(i);
    }
    return prevMatricesUpdates;
  }

  public void setWeightMatrix(int index, DoubleMatrix matrix) {
    Preconditions.checkArgument(
        0 <= index && index < this.weightMatrixList.size(), String.format(
            "index [%d] should be in range[%d, %d].", index, 0,
            this.weightMatrixList.size()));
    this.weightMatrixList.set(index, matrix);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);

    // read squash functions
    int squashingFunctionSize = input.readInt();
    this.squashingFunctionList = Lists.newArrayList();
    for (int i = 0; i < squashingFunctionSize; ++i) {
      this.squashingFunctionList.add(FunctionFactory
          .createDoubleFunction(WritableUtils.readString(input)));
    }

    // read weights and construct matrices of previous updates
    int numOfMatrices = input.readInt();
    this.weightMatrixList = Lists.newArrayList();
    this.prevWeightUpdatesList = Lists.newArrayList();
    for (int i = 0; i < numOfMatrices; ++i) {
      DoubleMatrix matrix = MatrixWritable.read(input);
      this.weightMatrixList.add(matrix);
      this.prevWeightUpdatesList.add(new DenseDoubleMatrix(
          matrix.getRowCount(), matrix.getColumnCount()));
    }

  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);

    // write squashing functions
    output.writeInt(this.squashingFunctionList.size());
    for (DoubleFunction aSquashingFunctionList : this.squashingFunctionList) {
      WritableUtils.writeString(output, aSquashingFunctionList
              .getFunctionName());
    }

    // write weight matrices
    output.writeInt(this.weightMatrixList.size());
    for (DoubleMatrix aWeightMatrixList : this.weightMatrixList) {
      MatrixWritable.write(aWeightMatrixList, output);
    }

    // DO NOT WRITE WEIGHT UPDATE
  }

  @Override
  public DoubleMatrix getWeightsByLayer(int layerIdx) {
    return this.weightMatrixList.get(layerIdx);
  }

  /**
   * Get the output of the model according to given feature instance.
   */
  @Override
  public DoubleVector getOutput(DoubleVector instance) {
    Preconditions.checkArgument(this.layerSizeList.get(0) - 1 == instance
        .getDimension(), String.format(
        "The dimension of input instance should be %d.",
        this.layerSizeList.get(0) - 1));
    // transform the features to another space
    DoubleVector transformedInstance = this.featureTransformer
        .transform(instance);
    // add bias feature
    DoubleVector instanceWithBias = new DenseDoubleVector(
        transformedInstance.getDimension() + 1);
    instanceWithBias.set(0, 0.99999); // set bias to be a little bit less than
                                      // 1.0
    for (int i = 1; i < instanceWithBias.getDimension(); ++i) {
      instanceWithBias.set(i, transformedInstance.get(i - 1));
    }

    List<DoubleVector> outputCache = getOutputInternal(instanceWithBias);
    // return the output of the last layer
    DoubleVector result = outputCache.get(outputCache.size() - 1);
    // remove bias
    return result.sliceUnsafe(1, result.getDimension() - 1);
  }

  /**
   * Calculate output internally, the intermediate output of each layer will be
   * stored.
   * 
   * @param instanceWithBias The instance contains the features.
   * @return Cached output of each layer.
   */
  public List<DoubleVector> getOutputInternal(DoubleVector instanceWithBias) {
    List<DoubleVector> outputCache = new ArrayList<DoubleVector>();
    // fill with instance
    DoubleVector intermediateOutput = instanceWithBias;
    outputCache.add(intermediateOutput);

    for (int i = 0; i < this.layerSizeList.size() - 1; ++i) {
      intermediateOutput = forward(i, intermediateOutput);
      outputCache.add(intermediateOutput);
    }
    return outputCache;
  }

  /**
   * Forward the calculation for one layer.
   * 
   * @param fromLayer The index of the previous layer.
   * @param intermediateOutput The intermediateOutput of previous layer.
   * @return
   */
  protected DoubleVector forward(int fromLayer, DoubleVector intermediateOutput) {
    DoubleMatrix weightMatrix = this.weightMatrixList.get(fromLayer);

    DoubleVector vec = weightMatrix.multiplyVectorUnsafe(intermediateOutput);
    vec = vec.applyToElements(this.squashingFunctionList.get(fromLayer));

    // add bias
    DoubleVector vecWithBias = new DenseDoubleVector(vec.getDimension() + 1);
    vecWithBias.set(0, 1);
    for (int i = 0; i < vec.getDimension(); ++i) {
      vecWithBias.set(i + 1, vec.get(i));
    }
    return vecWithBias;
  }

  /**
   * Train the model online.
   * 
   * @param trainingInstance
   */
  public void trainOnline(DoubleVector trainingInstance) {
    DoubleMatrix[] updateMatrices = this.trainByInstance(trainingInstance);
    this.updateWeightMatrices(updateMatrices);
  }

  @Override
  public DoubleMatrix[] trainByInstance(DoubleVector trainingInstance) {
    DoubleVector transformedVector = this.featureTransformer
        .transform(trainingInstance.sliceUnsafe(this.layerSizeList.get(0) - 1));

    int inputDimension = this.layerSizeList.get(0) - 1;
    int outputDimension;
    DoubleVector inputInstance = null;
    DoubleVector labels = null;
    if (this.learningStyle == LearningStyle.SUPERVISED) {
      outputDimension = this.layerSizeList.get(this.layerSizeList.size() - 1);
      // validate training instance
      Preconditions.checkArgument(
          inputDimension + outputDimension == trainingInstance.getDimension(),
          String
              .format(
                  "The dimension of training instance is %d, but requires %d.",
                  trainingInstance.getDimension(), inputDimension
                      + outputDimension));

      inputInstance = new DenseDoubleVector(this.layerSizeList.get(0));
      inputInstance.set(0, 1); // add bias
      // get the features from the transformed vector
      for (int i = 0; i < inputDimension; ++i) {
        inputInstance.set(i + 1, transformedVector.get(i));
      }
      // get the labels from the original training instance
      labels = trainingInstance.sliceUnsafe(inputInstance.getDimension() - 1,
          trainingInstance.getDimension() - 1);
    } else if (this.learningStyle == LearningStyle.UNSUPERVISED) {
      // labels are identical to input features
      outputDimension = inputDimension;
      // validate training instance
      Preconditions.checkArgument(inputDimension == trainingInstance
          .getDimension(), String.format(
          "The dimension of training instance is %d, but requires %d.",
          trainingInstance.getDimension(), inputDimension));

      inputInstance = new DenseDoubleVector(this.layerSizeList.get(0));
      inputInstance.set(0, 1); // add bias
      // get the features from the transformed vector
      for (int i = 0; i < inputDimension; ++i) {
        inputInstance.set(i + 1, transformedVector.get(i));
      }
      // get the labels by copying the transformed vector
      labels = transformedVector.deepCopy();
    }

    List<DoubleVector> internalResults = this.getOutputInternal(inputInstance);
    DoubleVector output = internalResults.get(internalResults.size() - 1);

    // get the training error
    calculateTrainingError(labels,
        output.deepCopy().sliceUnsafe(1, output.getDimension() - 1));

    if (this.trainingMethod.equals(TrainingMethod.GRADIENT_DESCENT)) {
      return this.trainByInstanceGradientDescent(labels, internalResults);
    } else {
      throw new IllegalArgumentException(
          String.format("Training method is not supported."));
    }
  }

  /**
   * Train by gradient descent. Get the updated weights using one training
   * instance.
   * 
   * @param trainingInstance
   * @return The weight update matrices.
   */
  private DoubleMatrix[] trainByInstanceGradientDescent(DoubleVector labels,
      List<DoubleVector> internalResults) {

    DoubleVector output = internalResults.get(internalResults.size() - 1);
    // initialize weight update matrices
    DenseDoubleMatrix[] weightUpdateMatrices = new DenseDoubleMatrix[this.weightMatrixList
        .size()];
    for (int m = 0; m < weightUpdateMatrices.length; ++m) {
      weightUpdateMatrices[m] = new DenseDoubleMatrix(this.weightMatrixList
          .get(m).getRowCount(), this.weightMatrixList.get(m).getColumnCount());
    }
    DoubleVector deltaVec = new DenseDoubleVector(
        this.layerSizeList.get(this.layerSizeList.size() - 1));

    DoubleFunction squashingFunction = this.squashingFunctionList
        .get(this.squashingFunctionList.size() - 1);

    DoubleMatrix lastWeightMatrix = this.weightMatrixList
        .get(this.weightMatrixList.size() - 1);
    for (int i = 0; i < deltaVec.getDimension(); ++i) {
      double costFuncDerivative = this.costFunction.applyDerivative(
          labels.get(i), output.get(i + 1));
      // add regularization
      costFuncDerivative += this.regularizationWeight
          * lastWeightMatrix.getRowVector(i).sum();
      deltaVec.set(i, costFuncDerivative);
      deltaVec.set(
          i,
          deltaVec.get(i)
              * squashingFunction.applyDerivative(output.get(i + 1)));
    }

    // start from previous layer of output layer
    for (int layer = this.layerSizeList.size() - 2; layer >= 0; --layer) {
      output = internalResults.get(layer);
      deltaVec = backpropagate(layer, deltaVec, internalResults,
          weightUpdateMatrices[layer]);
    }

    this.setPrevWeightMatrices(weightUpdateMatrices);

    return weightUpdateMatrices;
  }

  /**
   * Back-propagate the errors to from next layer to current layer. The weight
   * updated information will be stored in the weightUpdateMatrices, and the
   * delta of the prevLayer would be returned.
   * 
   * @param layer Index of current layer.
   * @param internalOutput Internal output of current layer.
   * @param deltaVec Delta of next layer.
   * @return
   */
  private DoubleVector backpropagate(int curLayerIdx,
      DoubleVector nextLayerDelta, List<DoubleVector> outputCache,
      DenseDoubleMatrix weightUpdateMatrix) {

    // get layer related information
    DoubleFunction squashingFunction = this.squashingFunctionList
        .get(curLayerIdx);
    DoubleVector curLayerOutput = outputCache.get(curLayerIdx);
    DoubleMatrix weightMatrix = this.weightMatrixList.get(curLayerIdx);
    DoubleMatrix prevWeightMatrix = this.prevWeightUpdatesList.get(curLayerIdx);

    // next layer is not output layer, remove the delta of bias neuron
    if (curLayerIdx != this.layerSizeList.size() - 2) {
      nextLayerDelta = nextLayerDelta.slice(1,
          nextLayerDelta.getDimension() - 1);
    }

    DoubleVector delta = weightMatrix.transpose()
        .multiplyVector(nextLayerDelta);
    for (int i = 0; i < delta.getDimension(); ++i) {
      delta.set(
          i,
          delta.get(i)
              * squashingFunction.applyDerivative(curLayerOutput.get(i)));
    }

    // update weights
    for (int i = 0; i < weightUpdateMatrix.getRowCount(); ++i) {
      for (int j = 0; j < weightUpdateMatrix.getColumnCount(); ++j) {
        weightUpdateMatrix.set(i, j,
            -learningRate * nextLayerDelta.get(i) * curLayerOutput.get(j)
                + this.momentumWeight * prevWeightMatrix.get(i, j));
      }
    }

    return delta;
  }

  @Override
  protected void trainInternal(Path dataInputPath,
      Map<String, String> trainingParams) throws IOException,
      InterruptedException, ClassNotFoundException {
    // add all training parameters to configuration
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : trainingParams.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    // if training parameters contains the model path, update the model path
    String modelPath = trainingParams.get("modelPath");
    if (modelPath != null) {
      this.modelPath = modelPath;
    }
    // modelPath must be set before training
    if (this.modelPath == null) {
      throw new IllegalArgumentException(
          "Please specify the modelPath for model, "
              + "either through setModelPath() or add 'modelPath' to the training parameters.");
    }

    conf.set("modelPath", this.modelPath);
    this.writeModelToFile();

    HamaConfiguration hamaConf = new HamaConfiguration(conf);

    // create job
    BSPJob job = new BSPJob(hamaConf, SmallLayeredNeuralNetworkTrainer.class);
    job.setJobName("Small scale Neural Network training");
    job.setJarByClass(SmallLayeredNeuralNetworkTrainer.class);
    job.setBspClass(SmallLayeredNeuralNetworkTrainer.class);
    job.setInputPath(dataInputPath);
    job.setInputFormat(org.apache.hama.bsp.SequenceFileInputFormat.class);
    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(VectorWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormat(org.apache.hama.bsp.NullOutputFormat.class);

    int numTasks = conf.getInt("tasks", 1);
    Log.info(String.format("Number of tasks: %d\n", numTasks));
    job.setNumBspTask(numTasks);
    job.waitForCompletion(true);

    // reload learned model
    Log.info(String.format("Reload model from %s.", this.modelPath));
    this.readFromModel();

  }

  @Override
  protected void calculateTrainingError(DoubleVector labels, DoubleVector output) {
    DoubleVector errors = labels.deepCopy().applyToElements(output,
        this.costFunction);
    this.trainingError = errors.sum();
  }

  /**
   * Get the squashing function of a specified layer.
   * 
   * @param idx
   * @return
   */
  public DoubleFunction getSquashingFunction(int idx) {
    return this.squashingFunctionList.get(idx);
  }

}
