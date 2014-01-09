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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.commons.io.MatrixWritable;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleFunction;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.ml.util.FeatureTransformer;
import org.mortbay.log.Log;

/**
 * SmallMultiLayerPerceptronBSP is a kind of multilayer perceptron whose
 * parameters can be fit into the memory of a single machine. This kind of model
 * can be trained and used more efficiently than the BigMultiLayerPerceptronBSP,
 * whose parameters are distributedly stored in multiple machines.
 * 
 * In general, it it is a multilayer perceptron that consists of one input
 * layer, multiple hidden layer and one output layer.
 * 
 * The number of neurons in the input layer should be consistent with the number
 * of features in the training instance. The number of neurons in the output
 * layer
 */
public final class SmallMultiLayerPerceptron extends MultiLayerPerceptron
    implements Writable {

  /* The in-memory weight matrix */
  private DenseDoubleMatrix[] weightMatrice;

  /* Previous weight updates, used for momentum */
  private DenseDoubleMatrix[] prevWeightUpdateMatrices;

  /**
   * {@inheritDoc}
   */
  public SmallMultiLayerPerceptron(double learningRate, double regularization,
      double momentum, String squashingFunctionName, String costFunctionName,
      int[] layerSizeArray) {
    super(learningRate, regularization, momentum, squashingFunctionName,
        costFunctionName, layerSizeArray);
    initializeWeightMatrix();
    this.initializePrevWeightUpdateMatrix();
  }

  /**
   * {@inheritDoc}
   */
  public SmallMultiLayerPerceptron(String modelPath) {
    super(modelPath);
    if (modelPath != null) {
      try {
        this.readFromModel();
        this.initializePrevWeightUpdateMatrix();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Initialize weight matrix using Gaussian distribution. Each weight is
   * initialized in range (-0.5, 0.5)
   */
  private void initializeWeightMatrix() {
    this.weightMatrice = new DenseDoubleMatrix[this.numberOfLayers - 1];
    // each layer contains one bias neuron
    for (int i = 0; i < this.numberOfLayers - 1; ++i) {
      // add weights for bias
      this.weightMatrice[i] = new DenseDoubleMatrix(this.layerSizeArray[i] + 1,
          this.layerSizeArray[i + 1]);

      this.weightMatrice[i].applyToElements(new DoubleFunction() {

        private final Random rnd = new Random();

        @Override
        public double apply(double value) {
          return rnd.nextDouble() - 0.5;
        }

        @Override
        public double applyDerivative(double value) {
          throw new UnsupportedOperationException("Not supported");
        }

      });

      // int rowCount = this.weightMatrice[i].getRowCount();
      // int colCount = this.weightMatrice[i].getColumnCount();
      // for (int row = 0; row < rowCount; ++row) {
      // for (int col = 0; col < colCount; ++col) {
      // this.weightMatrice[i].set(row, col, rnd.nextDouble() - 0.5);
      // }
      // }
    }
  }

  /**
   * Initial the momentum weight matrices.
   */
  private void initializePrevWeightUpdateMatrix() {
    this.prevWeightUpdateMatrices = new DenseDoubleMatrix[this.numberOfLayers - 1];
    for (int i = 0; i < this.prevWeightUpdateMatrices.length; ++i) {
      int row = this.layerSizeArray[i] + 1;
      int col = this.layerSizeArray[i + 1];
      this.prevWeightUpdateMatrices[i] = new DenseDoubleMatrix(row, col);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   * The model meta-data is stored in memory.
   */
  public DoubleVector outputWrapper(DoubleVector featureVector) {
    List<double[]> outputCache = this.outputInternal(featureVector);
    // the output of the last layer is the output of the MLP
    return new DenseDoubleVector(outputCache.get(outputCache.size() - 1));
  }

  private List<double[]> outputInternal(DoubleVector featureVector) {
    // store the output of the hidden layers and output layer, each array store
    // one layer
    List<double[]> outputCache = new ArrayList<double[]>();

    // start from the first hidden layer
    double[] intermediateResults = new double[this.layerSizeArray[0] + 1];
    if (intermediateResults.length - 1 != featureVector.getDimension()) {
      throw new IllegalStateException(
          "Input feature dimension incorrect! The dimension of input layer is "
              + (this.layerSizeArray[0] - 1)
              + ", but the dimension of input feature is "
              + featureVector.getDimension());
    }

    // fill with input features
    intermediateResults[0] = 1.0; // bias

    // transform the original features to another space
    featureVector = this.featureTransformer.transform(featureVector);

    for (int i = 0; i < featureVector.getDimension(); ++i) {
      intermediateResults[i + 1] = featureVector.get(i);
    }
    outputCache.add(intermediateResults);

    // forward the intermediate results to next layer
    for (int fromLayer = 0; fromLayer < this.numberOfLayers - 1; ++fromLayer) {
      intermediateResults = forward(fromLayer, intermediateResults);
      outputCache.add(intermediateResults);
    }

    return outputCache;
  }

  /**
   * Calculate the intermediate results of layer fromLayer + 1.
   * 
   * @param fromLayer The index of layer that forwards the intermediate results
   *          from.
   * @return
   */
  private double[] forward(int fromLayer, double[] intermediateResult) {
    int toLayer = fromLayer + 1;
    double[] results = null;
    int offset = 0;

    if (toLayer < this.layerSizeArray.length - 1) { // add bias if it is not
                                                    // output layer
      results = new double[this.layerSizeArray[toLayer] + 1];
      offset = 1;
      results[0] = 1.0; // the bias
    } else {
      results = new double[this.layerSizeArray[toLayer]]; // no bias
    }

    for (int neuronIdx = 0; neuronIdx < this.layerSizeArray[toLayer]; ++neuronIdx) {
      // aggregate the results from previous layer
      for (int prevNeuronIdx = 0; prevNeuronIdx < this.layerSizeArray[fromLayer] + 1; ++prevNeuronIdx) {
        results[neuronIdx + offset] += this.weightMatrice[fromLayer].get(
            prevNeuronIdx, neuronIdx) * intermediateResult[prevNeuronIdx];
      }
      // calculate via squashing function
      results[neuronIdx + offset] = this.squashingFunction
          .apply(results[neuronIdx + offset]);
    }

    return results;
  }

  /**
   * Get the updated weights using one training instance.
   * 
   * @param trainingInstance The trainingInstance is the concatenation of
   *          feature vector and class label vector.
   * @return The update of each weight.
   * @throws Exception
   */
  DenseDoubleMatrix[] trainByInstance(DoubleVector trainingInstance)
      throws Exception {
    // initialize weight update matrices
    DenseDoubleMatrix[] weightUpdateMatrices = new DenseDoubleMatrix[this.layerSizeArray.length - 1];
    for (int m = 0; m < weightUpdateMatrices.length; ++m) {
      weightUpdateMatrices[m] = new DenseDoubleMatrix(
          this.layerSizeArray[m] + 1, this.layerSizeArray[m + 1]);
    }

    if (trainingInstance == null) {
      return weightUpdateMatrices;
    }

    // transform the features (exclude the labels) to new space
    double[] trainingVec = trainingInstance.toArray();
    double[] trainingFeature = this.featureTransformer.transform(
        trainingInstance.sliceUnsafe(0, this.layerSizeArray[0] - 1)).toArray();
    double[] trainingLabels = Arrays.copyOfRange(trainingVec,
        this.layerSizeArray[0], trainingVec.length);

    DoubleVector trainingFeatureVec = new DenseDoubleVector(trainingFeature);
    List<double[]> outputCache = this.outputInternal(trainingFeatureVec);

    // calculate the delta of output layer
    double[] delta = new double[this.layerSizeArray[this.layerSizeArray.length - 1]];
    double[] outputLayerOutput = outputCache.get(outputCache.size() - 1);
    double[] lastHiddenLayerOutput = outputCache.get(outputCache.size() - 2);

    DenseDoubleMatrix prevWeightUpdateMatrix = this.prevWeightUpdateMatrices[this.prevWeightUpdateMatrices.length - 1];
    for (int j = 0; j < delta.length; ++j) {
      delta[j] = this.costFunction.applyDerivative(trainingLabels[j],
          outputLayerOutput[j]);
      // add regularization term
      if (this.regularization != 0.0) {
        double derivativeRegularization = 0.0;
        DenseDoubleMatrix weightMatrix = this.weightMatrice[this.weightMatrice.length - 1];
        for (int k = 0; k < this.layerSizeArray[this.layerSizeArray.length - 1]; ++k) {
          derivativeRegularization += weightMatrix.get(k, j);
        }
        derivativeRegularization /= this.layerSizeArray[this.layerSizeArray.length - 1];
        delta[j] += this.regularization * derivativeRegularization;
      }

      delta[j] *= this.squashingFunction.applyDerivative(outputLayerOutput[j]);

      // calculate the weight update matrix between the last hidden layer and
      // the output layer
      for (int i = 0; i < this.layerSizeArray[this.layerSizeArray.length - 2] + 1; ++i) {
        double updatedValue = -this.learningRate * delta[j]
            * lastHiddenLayerOutput[i];
        // add momentum
        updatedValue += this.momentum * prevWeightUpdateMatrix.get(i, j);
        weightUpdateMatrices[weightUpdateMatrices.length - 1].set(i, j,
            updatedValue);
      }
    }

    // calculate the delta for each hidden layer through back-propagation
    for (int l = this.layerSizeArray.length - 2; l >= 1; --l) {
      delta = backpropagate(l, delta, outputCache, weightUpdateMatrices);
    }

    return weightUpdateMatrices;
  }

  /**
   * Back-propagate the errors from nextLayer to prevLayer. The weight updated
   * information will be stored in the weightUpdateMatrices, and the delta of
   * the prevLayer would be returned.
   * 
   * @param curLayerIdx The layer index of the current layer.
   * @param nextLayerDelta The delta of the next layer.
   * @param outputCache The cache of the output of all the layers.
   * @param weightUpdateMatrices The weight update matrices.
   * @return The delta of the previous layer, will be used for next iteration of
   *         back-propagation.
   */
  private double[] backpropagate(int curLayerIdx, double[] nextLayerDelta,
      List<double[]> outputCache, DenseDoubleMatrix[] weightUpdateMatrices) {
    int prevLayerIdx = curLayerIdx - 1;
    double[] delta = new double[this.layerSizeArray[curLayerIdx]];
    double[] curLayerOutput = outputCache.get(curLayerIdx);
    double[] prevLayerOutput = outputCache.get(prevLayerIdx);

    // DenseDoubleMatrix prevWeightUpdateMatrix = this.prevWeightUpdateMatrices[curLayerIdx - 1];
    // for each neuron j in nextLayer, calculate the delta
    for (int j = 0; j < delta.length; ++j) {
      // aggregate delta from next layer
      for (int k = 0; k < nextLayerDelta.length; ++k) {
        double weight = this.weightMatrice[curLayerIdx].get(j, k);
        delta[j] += weight * nextLayerDelta[k];
      }
      delta[j] *= this.squashingFunction.applyDerivative(curLayerOutput[j + 1]);

      // calculate the weight update matrix between the previous layer and the
      // current layer
      for (int i = 0; i < weightUpdateMatrices[prevLayerIdx].getRowCount(); ++i) {
        double updatedValue = -this.learningRate * delta[j]
            * prevLayerOutput[i];
        // add momemtum
        // updatedValue += this.momentum * prevWeightUpdateMatrix.get(i, j);
        weightUpdateMatrices[prevLayerIdx].set(i, j, updatedValue);
      }
    }

    return delta;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void train(Path dataInputPath, Map<String, String> trainingParams)
      throws IOException, InterruptedException, ClassNotFoundException {
    // create the BSP training job
    Configuration conf = new Configuration();
    for (Map.Entry<String, String> entry : trainingParams.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    // put model related parameters
    if (modelPath == null || modelPath.trim().length() == 0) { // build model
                                                               // from scratch
      conf.set("MLPType", this.MLPType);
      conf.set("learningRate", "" + this.learningRate);
      conf.set("regularization", "" + this.regularization);
      conf.set("momentum", "" + this.momentum);
      conf.set("squashingFunctionName", this.squashingFunctionName);
      conf.set("costFunctionName", this.costFunctionName);
      StringBuilder layerSizeArraySb = new StringBuilder();
      for (int layerSize : this.layerSizeArray) {
        layerSizeArraySb.append(layerSize);
        layerSizeArraySb.append(' ');
      }
      conf.set("layerSizeArray", layerSizeArraySb.toString());
    }

    HamaConfiguration hamaConf = new HamaConfiguration(conf);

    BSPJob job = new BSPJob(hamaConf, SmallMLPTrainer.class);
    job.setJobName("Small scale MLP training");
    job.setJarByClass(SmallMLPTrainer.class);
    job.setBspClass(SmallMLPTrainer.class);
    job.setInputPath(dataInputPath);
    job.setInputFormat(org.apache.hama.bsp.SequenceFileInputFormat.class);
    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(VectorWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormat(org.apache.hama.bsp.NullOutputFormat.class);

    int numTasks = conf.getInt("tasks", 1);
    job.setNumBspTask(numTasks);
    job.waitForCompletion(true);

    // reload learned model
    Log.info(String.format("Reload model from %s.",
        trainingParams.get("modelPath")));
    this.modelPath = trainingParams.get("modelPath");
    this.readFromModel();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void readFields(DataInput input) throws IOException {
    this.MLPType = WritableUtils.readString(input);
    this.learningRate = input.readDouble();
    this.regularization = input.readDouble();
    this.momentum = input.readDouble();
    this.numberOfLayers = input.readInt();
    this.squashingFunctionName = WritableUtils.readString(input);
    this.costFunctionName = WritableUtils.readString(input);

    this.squashingFunction = FunctionFactory
        .createDoubleFunction(this.squashingFunctionName);
    this.costFunction = FunctionFactory
        .createDoubleDoubleFunction(this.costFunctionName);

    // read the number of neurons for each layer
    this.layerSizeArray = new int[this.numberOfLayers];
    for (int i = 0; i < numberOfLayers; ++i) {
      this.layerSizeArray[i] = input.readInt();
    }
    this.weightMatrice = new DenseDoubleMatrix[this.numberOfLayers - 1];
    for (int i = 0; i < numberOfLayers - 1; ++i) {
      this.weightMatrice[i] = (DenseDoubleMatrix) MatrixWritable.read(input);
    }

    // read feature transformer
    int bytesLen = input.readInt();
    byte[] featureTransformerBytes = new byte[bytesLen];
    for (int i = 0; i < featureTransformerBytes.length; ++i) {
      featureTransformerBytes[i] = input.readByte();
    }
    Class featureTransformerCls = (Class) SerializationUtils
        .deserialize(featureTransformerBytes);
    Constructor constructor = featureTransformerCls.getConstructors()[0];
    try {
      this.featureTransformer = (FeatureTransformer) constructor
          .newInstance(new Object[] {});
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    WritableUtils.writeString(output, MLPType);
    output.writeDouble(learningRate);
    output.writeDouble(regularization);
    output.writeDouble(momentum);
    output.writeInt(numberOfLayers);
    WritableUtils.writeString(output, squashingFunctionName);
    WritableUtils.writeString(output, costFunctionName);

    // write the number of neurons for each layer
    for (int i = 0; i < this.numberOfLayers; ++i) {
      output.writeInt(this.layerSizeArray[i]);
    }
    for (int i = 0; i < numberOfLayers - 1; ++i) {
      MatrixWritable matrixWritable = new MatrixWritable(this.weightMatrice[i]);
      matrixWritable.write(output);
    }

    // serialize the feature transformer
    Class<? extends FeatureTransformer> featureTransformerCls = this.featureTransformer
        .getClass();
    byte[] featureTransformerBytes = SerializationUtils
        .serialize(featureTransformerCls);
    output.writeInt(featureTransformerBytes.length);
    output.write(featureTransformerBytes);
  }

  /**
   * Read the model meta-data from the specified location.
   * 
   * @throws IOException
   */
  @Override
  protected void readFromModel() throws IOException {
    Configuration conf = new Configuration();
    try {
      URI uri = new URI(modelPath);
      FileSystem fs = FileSystem.get(uri, conf);
      FSDataInputStream is = new FSDataInputStream(fs.open(new Path(modelPath)));
      this.readFields(is);
      if (!this.MLPType.equals(this.getClass().getName())) {
        throw new IllegalStateException(String.format(
            "Model type incorrect, cannot load model '%s' for '%s'.",
            this.MLPType, this.getClass().getName()));
      }
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  /**
   * Write the model to file.
   * 
   * @throws IOException
   */
  @Override
  public void writeModelToFile(String modelPath) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream stream = fs.create(new Path(modelPath), true);
    this.write(stream);
    stream.close();
  }

  DenseDoubleMatrix[] getWeightMatrices() {
    return this.weightMatrice;
  }

  DenseDoubleMatrix[] getPrevWeightUpdateMatrices() {
    return this.prevWeightUpdateMatrices;
  }

  void setWeightMatrices(DenseDoubleMatrix[] newMatrices) {
    this.weightMatrice = newMatrices;
  }

  void setPrevWeightUpdateMatrices(
      DenseDoubleMatrix[] newPrevWeightUpdateMatrices) {
    this.prevWeightUpdateMatrices = newPrevWeightUpdateMatrices;
  }

  /**
   * Update the weight matrices with given updates.
   * 
   * @param updateMatrices The updates weights in matrix format.
   */
  void updateWeightMatrices(DenseDoubleMatrix[] updateMatrices) {
    for (int m = 0; m < this.weightMatrice.length; ++m) {
      this.weightMatrice[m] = (DenseDoubleMatrix) this.weightMatrice[m]
          .add(updateMatrices[m]);
    }
  }

  /**
   * Print out the weights.
   * 
   * @param mat
   * @return
   */
  static String weightsToString(DenseDoubleMatrix[] mat) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < mat.length; ++i) {
      sb.append(String.format("Matrix [%d]\n", i));
      double[][] values = mat[i].getValues();
      for (double[] value : values) {
        sb.append(Arrays.toString(value));
        sb.append('\n');
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  @Override
  protected String getTypeName() {
    return this.getClass().getName();
  }

}
