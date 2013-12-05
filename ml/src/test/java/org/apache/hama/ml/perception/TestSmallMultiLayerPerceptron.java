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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.commons.io.MatrixWritable;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.ml.util.DefaultFeatureTransformer;
import org.apache.hama.ml.util.FeatureTransformer;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestSmallMultiLayerPerceptron {

  /**
   * Write and read the parameters of MLP.
   */
  @Test
  public void testWriteReadMLP() {
    String modelPath = "/tmp/sampleModel-testWriteReadMLP.data";
    double learningRate = 0.3;
    double regularization = 0.0; // no regularization
    double momentum = 0; // no momentum
    String squashingFunctionName = "Sigmoid";
    String costFunctionName = "SquaredError";
    int[] layerSizeArray = new int[] { 3, 2, 2, 3 };
    MultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);
    FeatureTransformer transformer = new DefaultFeatureTransformer();
    mlp.setFeatureTransformer(transformer);
    try {
      mlp.writeModelToFile(modelPath);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      // read the meta-data
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      mlp = new SmallMultiLayerPerceptron(modelPath);
      assertEquals(mlp.getClass().getName(), mlp.getMLPType());
      assertEquals(learningRate, mlp.getLearningRate(), 0.001);
      assertEquals(regularization, mlp.isRegularization(), 0.001);
      assertEquals(layerSizeArray.length, mlp.getNumberOfLayers());
      assertEquals(momentum, mlp.getMomentum(), 0.001);
      assertEquals(squashingFunctionName, mlp.getSquashingFunctionName());
      assertEquals(costFunctionName, mlp.getCostFunctionName());
      assertArrayEquals(layerSizeArray, mlp.getLayerSizeArray());
      assertEquals(transformer.getClass().getName(), mlp.getFeatureTransformer().getClass().getName());
      // delete test file
      fs.delete(new Path(modelPath), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Test the output of an example MLP.
   */
  @Test
  public void testOutput() {
    // write the MLP meta-data manually
    String modelPath = "/tmp/sampleModel-testOutput.data";
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      FSDataOutputStream output = fs.create(new Path(modelPath), true);

      String MLPType = SmallMultiLayerPerceptron.class.getName();
      double learningRate = 0.5;
      double regularization = 0.0;
      double momentum = 0.1;
      String squashingFunctionName = "Sigmoid";
      String costFunctionName = "SquaredError";
      int[] layerSizeArray = new int[] { 3, 2, 3, 3 };
      int numberOfLayers = layerSizeArray.length;

      WritableUtils.writeString(output, MLPType);
      output.writeDouble(learningRate);
      output.writeDouble(regularization);
      output.writeDouble(momentum);
      output.writeInt(numberOfLayers);
      WritableUtils.writeString(output, squashingFunctionName);
      WritableUtils.writeString(output, costFunctionName);

      // write the number of neurons for each layer
      for (int i = 0; i < numberOfLayers; ++i) {
        output.writeInt(layerSizeArray[i]);
      }

      double[][] matrix01 = { // 4 by 2
      { 0.5, 0.2 }, { 0.1, 0.1 }, { 0.2, 0.5 }, { 0.1, 0.5 } };

      double[][] matrix12 = { // 3 by 3
      { 0.1, 0.2, 0.5 }, { 0.2, 0.5, 0.2 }, { 0.5, 0.5, 0.1 } };

      double[][] matrix23 = { // 4 by 3
      { 0.2, 0.5, 0.2 }, { 0.5, 0.1, 0.5 }, { 0.1, 0.2, 0.1 },
          { 0.1, 0.2, 0.5 } };

      DoubleMatrix[] matrices = { new DenseDoubleMatrix(matrix01),
          new DenseDoubleMatrix(matrix12), new DenseDoubleMatrix(matrix23) };
      for (DoubleMatrix mat : matrices) {
        MatrixWritable.write(mat, output);
      }

      // serialize the feature transformer
      FeatureTransformer transformer = new DefaultFeatureTransformer();
      Class<? extends FeatureTransformer> featureTransformerCls = transformer.getClass();
      byte[] featureTransformerBytes = SerializationUtils.serialize(featureTransformerCls);
      output.writeInt(featureTransformerBytes.length);
      output.write(featureTransformerBytes);
      
      output.close();

    } catch (IOException e) {
      e.printStackTrace();
    }

    // initial the mlp with existing model meta-data and get the output
    MultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(modelPath);
    DoubleVector input = new DenseDoubleVector(new double[] { 1, 2, 3 });
    try {
      DoubleVector result = mlp.output(input);
      assertArrayEquals(new double[] { 0.6636557, 0.7009963, 0.7213835 },
          result.toArray(), 0.0001);
    } catch (Exception e1) {
      e1.printStackTrace();
    }

    // delete meta-data
    try {
      FileSystem fs = FileSystem.get(conf);
      fs.delete(new Path(modelPath), true);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * Test training with squared error on the XOR problem.
   */
  @Test
  public void testTrainWithSquaredError() {
    // generate training data
    DoubleVector[] trainingData = new DenseDoubleVector[] {
        new DenseDoubleVector(new double[] { 0, 0, 0 }),
        new DenseDoubleVector(new double[] { 0, 1, 1 }),
        new DenseDoubleVector(new double[] { 1, 0, 1 }),
        new DenseDoubleVector(new double[] { 1, 1, 0 }) };

    // set parameters
    double learningRate = 0.3;
    double regularization = 0.02; // no regularization
    double momentum = 0; // no momentum
    String squashingFunctionName = "Sigmoid";
    String costFunctionName = "SquaredError";
    int[] layerSizeArray = new int[] { 2, 5, 1 };
    SmallMultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);

    try {
      // train by multiple instances
      Random rnd = new Random();
      for (int i = 0; i < 100000; ++i) {
        DenseDoubleMatrix[] weightUpdates = mlp
            .trainByInstance(trainingData[rnd.nextInt(4)]);
        mlp.updateWeightMatrices(weightUpdates);
      }

      // System.out.printf("Weight matrices: %s\n",
      // mlp.weightsToString(mlp.getWeightMatrices()));
      for (int i = 0; i < trainingData.length; ++i) {
        DenseDoubleVector testVec = (DenseDoubleVector) trainingData[i]
            .slice(2);
        double expected = trainingData[i].toArray()[2];
        double actual = mlp.output(testVec).toArray()[0];
        if (expected < 0.5 && actual >= 0.5 || expected >= 0.5 && actual < 0.5) {
          Log.info("Neural network failes to lear the XOR.");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Test training with cross entropy on the XOR problem.
   */
  @Test
  public void testTrainWithCrossEntropy() {
    // generate training data
    DoubleVector[] trainingData = new DenseDoubleVector[] {
        new DenseDoubleVector(new double[] { 0, 0, 0 }),
        new DenseDoubleVector(new double[] { 0, 1, 1 }),
        new DenseDoubleVector(new double[] { 1, 0, 1 }),
        new DenseDoubleVector(new double[] { 1, 1, 0 }) };

    // set parameters
    double learningRate = 0.3;
    double regularization = 0.0; // no regularization
    double momentum = 0; // no momentum
    String squashingFunctionName = "Sigmoid";
    String costFunctionName = "CrossEntropy";
    int[] layerSizeArray = new int[] { 2, 7, 1 };
    SmallMultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);

    try {
      // train by multiple instances
      Random rnd = new Random();
      for (int i = 0; i < 50000; ++i) {
        DenseDoubleMatrix[] weightUpdates = mlp
            .trainByInstance(trainingData[rnd.nextInt(4)]);
        mlp.updateWeightMatrices(weightUpdates);
      }

      // System.out.printf("Weight matrices: %s\n",
      // mlp.weightsToString(mlp.getWeightMatrices()));
      for (int i = 0; i < trainingData.length; ++i) {
        DenseDoubleVector testVec = (DenseDoubleVector) trainingData[i]
            .slice(2);
        double expected = trainingData[i].toArray()[2];
        double actual = mlp.output(testVec).toArray()[0];
        if (expected < 0.5 && actual >= 0.5 || expected >= 0.5 && actual < 0.5) {
          Log.info("Neural network failes to lear the XOR.");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Test training with regularizatiion.
   */
  @Test
  public void testWithRegularization() {
    // generate training data
    DoubleVector[] trainingData = new DenseDoubleVector[] {
        new DenseDoubleVector(new double[] { 0, 0, 0 }),
        new DenseDoubleVector(new double[] { 0, 1, 1 }),
        new DenseDoubleVector(new double[] { 1, 0, 1 }),
        new DenseDoubleVector(new double[] { 1, 1, 0 }) };

    // set parameters
    double learningRate = 0.3;
    double regularization = 0.02; // regularization should be a tiny number
    double momentum = 0; // no momentum
    String squashingFunctionName = "Sigmoid";
    String costFunctionName = "CrossEntropy";
    int[] layerSizeArray = new int[] { 2, 7, 1 };
    SmallMultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);

    try {
      // train by multiple instances
      Random rnd = new Random();
      for (int i = 0; i < 20000; ++i) {
        DenseDoubleMatrix[] weightUpdates = mlp
            .trainByInstance(trainingData[rnd.nextInt(4)]);
        mlp.updateWeightMatrices(weightUpdates);
      }

      // System.out.printf("Weight matrices: %s\n",
      // mlp.weightsToString(mlp.getWeightMatrices()));
      for (int i = 0; i < trainingData.length; ++i) {
        DenseDoubleVector testVec = (DenseDoubleVector) trainingData[i]
            .slice(2);
        double expected = trainingData[i].toArray()[2];
        double actual = mlp.output(testVec).toArray()[0];
        if (expected < 0.5 && actual >= 0.5 || expected >= 0.5 && actual < 0.5) {
          Log.info("Neural network failes to lear the XOR.");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Test training with momentum. The MLP can converge faster.
   */
  @Test
  public void testWithMomentum() {
    // generate training data
    DoubleVector[] trainingData = new DenseDoubleVector[] {
        new DenseDoubleVector(new double[] { 0, 0, 0 }),
        new DenseDoubleVector(new double[] { 0, 1, 1 }),
        new DenseDoubleVector(new double[] { 1, 0, 1 }),
        new DenseDoubleVector(new double[] { 1, 1, 0 }) };

    // set parameters
    double learningRate = 0.3;
    double regularization = 0.02; // regularization should be a tiny number
    double momentum = 0.5; // no momentum
    String squashingFunctionName = "Sigmoid";
    String costFunctionName = "CrossEntropy";
    int[] layerSizeArray = new int[] { 2, 7, 1 };
    SmallMultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);

    try {
      // train by multiple instances
      Random rnd = new Random();
      for (int i = 0; i < 5000; ++i) {
        DenseDoubleMatrix[] weightUpdates = mlp
            .trainByInstance(trainingData[rnd.nextInt(4)]);
        mlp.updateWeightMatrices(weightUpdates);
      }

      // System.out.printf("Weight matrices: %s\n",
      // mlp.weightsToString(mlp.getWeightMatrices()));
      for (int i = 0; i < trainingData.length; ++i) {
        DenseDoubleVector testVec = (DenseDoubleVector) trainingData[i]
            .slice(2);
        double expected = trainingData[i].toArray()[2];
        double actual = mlp.output(testVec).toArray()[0];
        if (expected < 0.5 && actual >= 0.5 || expected >= 0.5 && actual < 0.5) {
          Log.info("Neural network failes to lear the XOR.");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testByRunningJobs() {
    this.testTrainingByXOR();
    this.testFeatureTransformer();
  }

  /**
   * Test the XOR problem.
   */
  public void testTrainingByXOR() {
    // write in some training instances
    Configuration conf = new Configuration();
    String strDataPath = "/tmp/xor-training-by-xor";
    Path dataPath = new Path(strDataPath);

    // generate training data
    DoubleVector[] trainingData = new DenseDoubleVector[] {
        new DenseDoubleVector(new double[] { 0, 0, 0 }),
        new DenseDoubleVector(new double[] { 0, 1, 1 }),
        new DenseDoubleVector(new double[] { 1, 0, 1 }),
        new DenseDoubleVector(new double[] { 1, 1, 0 }) };

    try {
      URI uri = new URI(strDataPath);
      FileSystem fs = FileSystem.get(uri, conf);
      fs.delete(dataPath, true);
      if (!fs.exists(dataPath)) {
        fs.createNewFile(dataPath);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
            dataPath, LongWritable.class, VectorWritable.class);

        for (int i = 0; i < 1000; ++i) {
          VectorWritable vecWritable = new VectorWritable(trainingData[i % 4]);
          writer.append(new LongWritable(i), vecWritable);
        }
        writer.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    // begin training
    String modelPath = "/tmp/xorModel-training-by-xor.data";
    double learningRate = 0.6;
    double regularization = 0.02; // no regularization
    double momentum = 0.3; // no momentum
    String squashingFunctionName = "Tanh";
    String costFunctionName = "SquaredError";
    int[] layerSizeArray = new int[] { 2, 5, 1 };
    SmallMultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);

    Map<String, String> trainingParams = new HashMap<String, String>();
    trainingParams.put("training.iteration", "2000");
    trainingParams.put("training.mode", "minibatch.gradient.descent");
    trainingParams.put("training.batch.size", "100");
    trainingParams.put("tasks", "3");
    trainingParams.put("modelPath", modelPath);

    try {
      mlp.train(dataPath, trainingParams);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // test the model
    for (int i = 0; i < trainingData.length; ++i) {
      DenseDoubleVector testVec = (DenseDoubleVector) trainingData[i].slice(2);
      try {
        double expected = trainingData[i].toArray()[2];
        double actual = mlp.output(testVec).toArray()[0];
        if (expected < 0.5 && actual >= 0.5 || expected >= 0.5 && actual < 0.5) {
          Log.info("Neural network failes to lear the XOR.");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  /**
   * Use transformer to extract the first half features of the original features.
   */
  public void testFeatureTransformer() {
 // write in some training instances
    Configuration conf = new Configuration();
    String strDataPath = "/tmp/xor-training-by-xor";
    Path dataPath = new Path(strDataPath);

    // generate training data
    DoubleVector[] trainingData = new DenseDoubleVector[] {
        new DenseDoubleVector(new double[] { 0, 0, 0 }),
        new DenseDoubleVector(new double[] { 0, 1, 1 }),
        new DenseDoubleVector(new double[] { 1, 0, 1 }),
        new DenseDoubleVector(new double[] { 1, 1, 0 }) };
    
    try {
      URI uri = new URI(strDataPath);
      FileSystem fs = FileSystem.get(uri, conf);
      fs.delete(dataPath, true);
      if (!fs.exists(dataPath)) {
        fs.createNewFile(dataPath);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
            dataPath, LongWritable.class, VectorWritable.class);

        for (int i = 0; i < 1000; ++i) {
          VectorWritable vecWritable = new VectorWritable(trainingData[i % 4]);
          writer.append(new LongWritable(i), vecWritable);
        }
        writer.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    // begin training
    String modelPath = "/tmp/xorModel-training-by-xor.data";
    double learningRate = 0.6;
    double regularization = 0.02; // no regularization
    double momentum = 0.3; // no momentum
    String squashingFunctionName = "Tanh";
    String costFunctionName = "SquaredError";
    int[] layerSizeArray = new int[] { 1, 5, 1 };
    SmallMultiLayerPerceptron mlp = new SmallMultiLayerPerceptron(learningRate,
        regularization, momentum, squashingFunctionName, costFunctionName,
        layerSizeArray);
    
    mlp.setFeatureTransformer(new FeatureTransformer() {

      @Override
      public DoubleVector transform(DoubleVector originalFeatures) {
        return originalFeatures.sliceUnsafe(originalFeatures.getDimension() / 2);
      }
      
    });

    Map<String, String> trainingParams = new HashMap<String, String>();
    trainingParams.put("training.iteration", "2000");
    trainingParams.put("training.mode", "minibatch.gradient.descent");
    trainingParams.put("training.batch.size", "100");
    trainingParams.put("tasks", "3");
    trainingParams.put("modelPath", modelPath);

    try {
      mlp.train(dataPath, trainingParams);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
