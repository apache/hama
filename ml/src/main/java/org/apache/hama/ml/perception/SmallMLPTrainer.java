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
import java.util.Arrays;
import java.util.BitSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.ml.ann.NeuralNetworkTrainer;

/**
 * The perceptron trainer for small scale MLP.
 */
class SmallMLPTrainer extends NeuralNetworkTrainer {

  /* used by master only, check whether all slaves finishes reading */
  private BitSet statusSet;

  private int numTrainingInstanceRead = 0;
  /* Once reader reaches the EOF, the training procedure would be terminated */
  private boolean terminateTraining = false;

  private SmallMultiLayerPerceptron inMemoryPerceptron;

  private int[] layerSizeArray;

  @Override
  protected void extraSetup(
      BSPPeer<LongWritable, VectorWritable, NullWritable, NullWritable, MLPMessage> peer) {

    // obtain parameters
    this.trainingMode = conf.get("training.mode", "minibatch.gradient.descent");
    // mini-batch by default
    this.batchSize = conf.getInt("training.batch.size", 100);

    this.statusSet = new BitSet(peer.getConfiguration().getInt("tasks", 1));

    String outputModelPath = conf.get("modelPath");
    if (outputModelPath == null || outputModelPath.trim().length() == 0) {
      try {
        throw new Exception("Please specify output model path.");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    String modelPath = conf.get("existingModelPath");
    // build model from scratch
    if (modelPath == null || modelPath.trim().length() == 0) {
      double learningRate = Double.parseDouble(conf.get("learningRate"));
      double regularization = Double.parseDouble(conf.get("regularization"));
      double momentum = Double.parseDouble(conf.get("momentum"));
      String squashingFunctionName = conf.get("squashingFunctionName");
      String costFunctionName = conf.get("costFunctionName");
      String[] layerSizeArrayStr = conf.get("layerSizeArray").trim().split(" ");
      this.layerSizeArray = new int[layerSizeArrayStr.length];
      for (int i = 0; i < this.layerSizeArray.length; ++i) {
        this.layerSizeArray[i] = Integer.parseInt(layerSizeArrayStr[i]);
      }

      this.inMemoryPerceptron = new SmallMultiLayerPerceptron(learningRate,
          regularization, momentum, squashingFunctionName, costFunctionName,
          layerSizeArray);
      LOG.info("Training model from scratch.");
    } else { // read model from existing data
      this.inMemoryPerceptron = new SmallMultiLayerPerceptron(modelPath);
      LOG.info("Training with existing model.");
    }

  }

  @Override
  protected void extraCleanup(
      BSPPeer<LongWritable, VectorWritable, NullWritable, NullWritable, MLPMessage> peer) {
    LOG.info(String.format("Task %d totally read %d records.\n",
        peer.getPeerIndex(), this.numTrainingInstanceRead));
    // master write learned model to disk
    if (peer.getPeerIndex() == 0) {
      try {
        LOG.info(String.format("Master write learned model to %s\n",
            conf.get("modelPath")));
        this.inMemoryPerceptron.writeModelToFile(conf.get("modelPath"));
      } catch (IOException e) {
        System.err.println("Please set a correct model path.");
      }
    }
  }

  @Override
  public void bsp(
      BSPPeer<LongWritable, VectorWritable, NullWritable, NullWritable, MLPMessage> peer)
      throws IOException, SyncException, InterruptedException {
    LOG.info("Start training...");
    if (trainingMode.equalsIgnoreCase("minibatch.gradient.descent")) {
      LOG.info("Training Mode: minibatch.gradient.descent");
      trainByMinibatch(peer);
    }

    LOG.info(String.format("Task %d finished.", peer.getPeerIndex()));
  }

  /**
   * Train the MLP with stochastic gradient descent.
   * 
   * @param peer
   * @throws IOException
   * @throws SyncException
   * @throws InterruptedException
   */
  private void trainByMinibatch(
      BSPPeer<LongWritable, VectorWritable, NullWritable, NullWritable, MLPMessage> peer)
      throws IOException, SyncException, InterruptedException {

    int maxIteration = conf.getInt("training.iteration", 1);
    LOG.info("# of Training Iteration: " + maxIteration);

    for (int i = 0; i < maxIteration; ++i) {
      if (peer.getPeerIndex() == 0) {
        LOG.info(String.format("Iteration [%d] begins...", i));
      }
      peer.reopenInput();
      // reset status
      if (peer.getPeerIndex() == 0) {
        this.statusSet = new BitSet(peer.getConfiguration().getInt("tasks", 1));
      }
      this.terminateTraining = false;
      peer.sync();
      while (true) {
        // each slate task updates weights according to training data
        boolean terminate = updateWeights(peer);
        peer.sync();

        // master merges the updates
        if (peer.getPeerIndex() == 0) {
          mergeUpdate(peer);
        }
        peer.sync();

        if (terminate) {
          break;
        }
      }

    }

  }

  /**
   * Merge the updates from slaves task.
   * 
   * @param peer
   * @throws IOException
   */
  private void mergeUpdate(
      BSPPeer<LongWritable, VectorWritable, NullWritable, NullWritable, MLPMessage> peer)
      throws IOException {
    // initialize the cache
    DenseDoubleMatrix[] mergedUpdates = this.getZeroWeightMatrices();

    int numOfPartitions = peer.getNumCurrentMessages();

    // aggregates the weights update
    while (peer.getNumCurrentMessages() > 0) {
      SmallMLPMessage message = (SmallMLPMessage) peer.getCurrentMessage();
      if (message.isTerminated()) {
        this.statusSet.set(message.getOwner());
      }

      DenseDoubleMatrix[] weightUpdates = message.getWeightUpdatedMatrices();
      for (int m = 0; m < mergedUpdates.length; ++m) {
        mergedUpdates[m] = (DenseDoubleMatrix) mergedUpdates[m]
            .add(weightUpdates[m]);
      }
    }

    if (numOfPartitions != 0) {
      // calculate the global mean (the mean of batches from all slave tasks) of
      // the weight updates
      for (int m = 0; m < mergedUpdates.length; ++m) {
        mergedUpdates[m] = (DenseDoubleMatrix) mergedUpdates[m]
            .divide(numOfPartitions);
      }

      // check if all tasks finishes reading data
      if (this.statusSet.cardinality() == conf.getInt("tasks", 1)) {
        this.terminateTraining = true;
      }

      // update the weight matrices
      this.inMemoryPerceptron.updateWeightMatrices(mergedUpdates);
      this.inMemoryPerceptron.setPrevWeightUpdateMatrices(mergedUpdates);
    }

    // broadcast updated weight matrices
    for (String peerName : peer.getAllPeerNames()) {
      SmallMLPMessage msg = new SmallMLPMessage(peer.getPeerIndex(),
          this.terminateTraining, this.inMemoryPerceptron.getWeightMatrices(),
          this.inMemoryPerceptron.getPrevWeightUpdateMatrices());
      peer.send(peerName, msg);
    }

  }

  /**
   * Train the MLP with training data.
   * 
   * @param peer
   * @return Whether terminates.
   * @throws IOException
   */
  private boolean updateWeights(
      BSPPeer<LongWritable, VectorWritable, NullWritable, NullWritable, MLPMessage> peer)
      throws IOException {
    // receive update message sent by master
    if (peer.getNumCurrentMessages() > 0) {
      SmallMLPMessage message = (SmallMLPMessage) peer.getCurrentMessage();
      this.terminateTraining = message.isTerminated();
      // each slave renew its weight matrices
      this.inMemoryPerceptron.setWeightMatrices(message
          .getWeightUpdatedMatrices());
      this.inMemoryPerceptron.setPrevWeightUpdateMatrices(message
          .getPrevWeightsUpdatedMatrices());
      if (this.terminateTraining) {
        return true;
      }
    }

    // update weight according to training data
    DenseDoubleMatrix[] weightUpdates = this.getZeroWeightMatrices();

    int count = 0;
    LongWritable recordId = new LongWritable();
    VectorWritable trainingInstance = new VectorWritable();
    boolean hasMore = false;
    while (count++ < this.batchSize) {
      hasMore = peer.readNext(recordId, trainingInstance);

      try {
        DenseDoubleMatrix[] singleTrainingInstanceUpdates = this.inMemoryPerceptron
            .trainByInstance(trainingInstance.getVector());
        // aggregate the updates
        for (int m = 0; m < weightUpdates.length; ++m) {
          weightUpdates[m] = (DenseDoubleMatrix) weightUpdates[m]
              .add(singleTrainingInstanceUpdates[m]);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      ++numTrainingInstanceRead;
      if (!hasMore) {
        break;
      }
    }

    // calculate the local mean (the mean of the local batch) of weight updates
    for (int m = 0; m < weightUpdates.length; ++m) {
      weightUpdates[m] = (DenseDoubleMatrix) weightUpdates[m].divide(count);
    }

    LOG.info(String.format("Task %d has read %d records.", peer.getPeerIndex(),
        this.numTrainingInstanceRead));

    // send the weight updates to master task
    SmallMLPMessage message = new SmallMLPMessage(peer.getPeerIndex(),
        !hasMore, weightUpdates);
    peer.send(peer.getPeerName(0), message); // send status to master

    return !hasMore;
  }

  /**
   * Initialize the weight matrices.
   */
  private DenseDoubleMatrix[] getZeroWeightMatrices() {
    DenseDoubleMatrix[] weightUpdateCache = new DenseDoubleMatrix[this.layerSizeArray.length - 1];
    // initialize weight matrix each layer
    for (int i = 0; i < weightUpdateCache.length; ++i) {
      weightUpdateCache[i] = new DenseDoubleMatrix(this.layerSizeArray[i] + 1,
          this.layerSizeArray[i + 1]);
    }
    return weightUpdateCache;
  }

  /**
   * Print out the weights.
   * 
   * @param mat
   * @return
   */
  protected static String weightsToString(DenseDoubleMatrix[] mat) {
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

}
