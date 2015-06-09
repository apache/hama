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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.ml.MLTestBase;
import org.apache.hama.ml.ann.AbstractLayeredNeuralNetwork.LearningStyle;
import org.apache.hama.ml.ann.AbstractLayeredNeuralNetwork.TrainingMethod;
import org.apache.hama.ml.util.DefaultFeatureTransformer;
import org.apache.hama.ml.util.FeatureTransformer;
import org.junit.Test;
import org.mortbay.log.Log;

/**
 * Test the functionality of SmallLayeredNeuralNetwork.
 * 
 */
public class TestSmallLayeredNeuralNetwork extends MLTestBase {

  @Test
  public void testReadWrite() {
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.addLayer(2, false,
        FunctionFactory.createDoubleFunction("IdentityFunction"));
    ann.addLayer(5, false,
        FunctionFactory.createDoubleFunction("IdentityFunction"));
    ann.addLayer(1, true,
        FunctionFactory.createDoubleFunction("IdentityFunction"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    double learningRate = 0.2;
    ann.setLearningRate(learningRate);
    double momentumWeight = 0.5;
    ann.setMomemtumWeight(momentumWeight);
    double regularizationWeight = 0.05;
    ann.setRegularizationWeight(regularizationWeight);
    // intentionally initialize all weights to 0.5
    DoubleMatrix[] matrices = new DenseDoubleMatrix[2];
    matrices[0] = new DenseDoubleMatrix(5, 3, 0.2);
    matrices[1] = new DenseDoubleMatrix(1, 6, 0.8);
    ann.setWeightMatrices(matrices);
    ann.setLearningStyle(LearningStyle.UNSUPERVISED);
    
    FeatureTransformer defaultFeatureTransformer = new DefaultFeatureTransformer();
    ann.setFeatureTransformer(defaultFeatureTransformer);
    

    // write to file
    String modelPath = "/tmp/testSmallLayeredNeuralNetworkReadWrite";
    ann.setModelPath(modelPath);
    try {
      ann.writeModelToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // read from file
    SmallLayeredNeuralNetwork annCopy = new SmallLayeredNeuralNetwork(modelPath);
    assertEquals(annCopy.getClass().getSimpleName(), annCopy.getModelType());
    assertEquals(modelPath, annCopy.getModelPath());
    assertEquals(learningRate, annCopy.getLearningRate(), 0.000001);
    assertEquals(momentumWeight, annCopy.getMomemtumWeight(), 0.000001);
    assertEquals(regularizationWeight, annCopy.getRegularizationWeight(),
        0.000001);
    assertEquals(TrainingMethod.GRADIENT_DESCENT, annCopy.getTrainingMethod());
    assertEquals(LearningStyle.UNSUPERVISED, annCopy.getLearningStyle());

    // compare weights
    DoubleMatrix[] weightsMatrices = annCopy.getWeightMatrices();
    for (int i = 0; i < weightsMatrices.length; ++i) {
      DoubleMatrix expectMat = matrices[i];
      DoubleMatrix actualMat = weightsMatrices[i];
      for (int j = 0; j < expectMat.getRowCount(); ++j) {
        for (int k = 0; k < expectMat.getColumnCount(); ++k) {
          assertEquals(expectMat.get(j, k), actualMat.get(j, k), 0.000001);
        }
      }
    }
    
    FeatureTransformer copyTransformer = annCopy.getFeatureTransformer();
    assertEquals(defaultFeatureTransformer.getClass().getName(), copyTransformer.getClass().getName());
  }

  @Test
  /**
   * Test the forward functionality.
   */
  public void testOutput() {
    // first network
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.addLayer(2, false,
        FunctionFactory.createDoubleFunction("IdentityFunction"));
    ann.addLayer(5, false,
        FunctionFactory.createDoubleFunction("IdentityFunction"));
    ann.addLayer(1, true,
        FunctionFactory.createDoubleFunction("IdentityFunction"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    ann.setLearningRate(0.1);
    // intentionally initialize all weights to 0.5
    DoubleMatrix[] matrices = new DenseDoubleMatrix[2];
    matrices[0] = new DenseDoubleMatrix(5, 3, 0.5);
    matrices[1] = new DenseDoubleMatrix(1, 6, 0.5);
    ann.setWeightMatrices(matrices);

    double[] arr = new double[] { 0, 1 };
    DoubleVector training = new DenseDoubleVector(arr);
    DoubleVector result = ann.getOutput(training);
    assertEquals(1, result.getDimension());
    // assertEquals(3, result.get(0), 0.000001);

    // second network
    SmallLayeredNeuralNetwork ann2 = new SmallLayeredNeuralNetwork();
    ann2.addLayer(2, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann2.addLayer(3, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann2.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann2.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    ann2.setLearningRate(0.3);
    // intentionally initialize all weights to 0.5
    DoubleMatrix[] matrices2 = new DenseDoubleMatrix[2];
    matrices2[0] = new DenseDoubleMatrix(3, 3, 0.5);
    matrices2[1] = new DenseDoubleMatrix(1, 4, 0.5);
    ann2.setWeightMatrices(matrices2);

    double[] test = { 0, 0 };
    double[] result2 = { 0.807476 };

    DoubleVector vec = ann2.getOutput(new DenseDoubleVector(test));
    assertArrayEquals(result2, vec.toArray(), 0.000001);

    SmallLayeredNeuralNetwork ann3 = new SmallLayeredNeuralNetwork();
    ann3.addLayer(2, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann3.addLayer(3, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann3.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann3.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    ann3.setLearningRate(0.3);
    // intentionally initialize all weights to 0.5
    DoubleMatrix[] initMatrices = new DenseDoubleMatrix[2];
    initMatrices[0] = new DenseDoubleMatrix(3, 3, 0.5);
    initMatrices[1] = new DenseDoubleMatrix(1, 4, 0.5);
    ann3.setWeightMatrices(initMatrices);

    double[] instance = { 0, 1 };
    DoubleVector output = ann3.getOutput(new DenseDoubleVector(instance));
    assertEquals(0.8315410, output.get(0), 0.000001);
  }

  @Test
  public void testXORlocal() {
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.addLayer(2, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(3, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    ann.setLearningRate(0.5);
    ann.setMomemtumWeight(0.0);

    int iterations = 50000; // iteration should be set to a very large number
    double[][] instances = { { 0, 1, 1 }, { 0, 0, 0 }, { 1, 0, 1 }, { 1, 1, 0 } };
    for (int i = 0; i < iterations; ++i) {
      DoubleMatrix[] matrices = null;
      for (int j = 0; j < instances.length; ++j) {
        matrices = ann.trainByInstance(new DenseDoubleVector(instances[j
            % instances.length]));
        ann.updateWeightMatrices(matrices);
      }
    }

    for (int i = 0; i < instances.length; ++i) {
      DoubleVector input = new DenseDoubleVector(instances[i]).slice(2);
      // the expected output is the last element in array
      double result = instances[i][2];
      double actual = ann.getOutput(input).get(0);
      if (result < 0.5 && actual >= 0.5 || result >= 0.5 && actual < 0.5) {
        Log.info("Neural network failes to lear the XOR.");
      }
    }

    // write model into file and read out
    String modelPath = "/tmp/testSmallLayeredNeuralNetworkXORLocal";
    ann.setModelPath(modelPath);
    try {
      ann.writeModelToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    SmallLayeredNeuralNetwork annCopy = new SmallLayeredNeuralNetwork(modelPath);
    // test on instances
    for (int i = 0; i < instances.length; ++i) {
      DoubleVector input = new DenseDoubleVector(instances[i]).slice(2);
      // the expected output is the last element in array
      double result = instances[i][2];
      double actual = annCopy.getOutput(input).get(0);
      if (result < 0.5 && actual >= 0.5 || result >= 0.5 && actual < 0.5) {
        Log.info("Neural network failes to lear the XOR.");
      }
    }
  }

  @Test
  public void testXORWithMomentum() {
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.addLayer(2, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(3, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    ann.setLearningRate(0.6);
    ann.setMomemtumWeight(0.3);

    int iterations = 2000; // iteration should be set to a very large number
    double[][] instances = { { 0, 1, 1 }, { 0, 0, 0 }, { 1, 0, 1 }, { 1, 1, 0 } };
    for (int i = 0; i < iterations; ++i) {
      for (int j = 0; j < instances.length; ++j) {
        ann.trainOnline(new DenseDoubleVector(instances[j % instances.length]));
      }
    }

    for (int i = 0; i < instances.length; ++i) {
      DoubleVector input = new DenseDoubleVector(instances[i]).slice(2);
      // the expected output is the last element in array
      double result = instances[i][2];
      double actual = ann.getOutput(input).get(0);
      if (result < 0.5 && actual >= 0.5 || result >= 0.5 && actual < 0.5) {
        Log.info("Neural network failes to lear the XOR.");
      }
    }

    // write model into file and read out
    String modelPath = "/tmp/testSmallLayeredNeuralNetworkXORLocalWithMomentum";
    ann.setModelPath(modelPath);
    try {
      ann.writeModelToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    SmallLayeredNeuralNetwork annCopy = new SmallLayeredNeuralNetwork(modelPath);
    // test on instances
    for (int i = 0; i < instances.length; ++i) {
      DoubleVector input = new DenseDoubleVector(instances[i]).slice(2);
      // the expected output is the last element in array
      double result = instances[i][2];
      double actual = annCopy.getOutput(input).get(0);
      if (result < 0.5 && actual >= 0.5 || result >= 0.5 && actual < 0.5) {
        Log.info("Neural network failes to lear the XOR.");
      }
    }
  }

  @Test
  public void testXORLocalWithRegularization() {
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.addLayer(2, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(3, false, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("SquaredError"));
    ann.setLearningRate(0.7);
    ann.setMomemtumWeight(0.5);
    ann.setRegularizationWeight(0.002);

    int iterations = 5000; // iteration should be set to a very large number
    double[][] instances = { { 0, 1, 1 }, { 0, 0, 0 }, { 1, 0, 1 }, { 1, 1, 0 } };
    for (int i = 0; i < iterations; ++i) {
      for (int j = 0; j < instances.length; ++j) {
        ann.trainOnline(new DenseDoubleVector(instances[j % instances.length]));
      }
    }

    for (int i = 0; i < instances.length; ++i) {
      DoubleVector input = new DenseDoubleVector(instances[i]).slice(2);
      // the expected output is the last element in array
      double result = instances[i][2];
      double actual = ann.getOutput(input).get(0);
      if (result < 0.5 && actual >= 0.5 || result >= 0.5 && actual < 0.5) {
        Log.info("Neural network failes to lear the XOR.");
      }
    }

    // write model into file and read out
    String modelPath = "/tmp/testSmallLayeredNeuralNetworkXORLocalWithRegularization";
    ann.setModelPath(modelPath);
    try {
      ann.writeModelToFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    SmallLayeredNeuralNetwork annCopy = new SmallLayeredNeuralNetwork(modelPath);
    // test on instances
    for (int i = 0; i < instances.length; ++i) {
      DoubleVector input = new DenseDoubleVector(instances[i]).slice(2);
      // the expected output is the last element in array
      double result = instances[i][2];
      double actual = annCopy.getOutput(input).get(0);
      if (result < 0.5 && actual >= 0.5 || result >= 0.5 && actual < 0.5) {
        Log.info("Neural network failes to lear the XOR.");
      }
    }
  }

  @Test
  public void testTwoClassClassification() {
    // use logistic regression data
    String filepath = "src/test/resources/logistic_regression_data.txt";
    List<double[]> instanceList = new ArrayList<double[]>();

    try {
      BufferedReader br = new BufferedReader(new FileReader(filepath));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.trim().split(",");
        double[] instance = new double[tokens.length];
        for (int i = 0; i < tokens.length; ++i) {
          instance[i] = Double.parseDouble(tokens[i]);
        }
        instanceList.add(instance);
      }
      br.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    zeroOneNormalization(instanceList, instanceList.get(0).length - 1);
    
    int dimension = instanceList.get(0).length - 1;

    // divide dataset into training and testing
    List<double[]> testInstances = new ArrayList<double[]>();
    testInstances.addAll(instanceList.subList(instanceList.size() - 100,
        instanceList.size()));
    List<double[]> trainingInstances = instanceList.subList(0,
        instanceList.size() - 100);

    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.setLearningRate(0.001);
    ann.setMomemtumWeight(0.1);
    ann.setRegularizationWeight(0.01);
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("CrossEntropy"));

    long start = new Date().getTime();
    int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
      for (double[] trainingInstance : trainingInstances) {
        ann.trainOnline(new DenseDoubleVector(trainingInstance));
      }
    }
    long end = new Date().getTime();
    Log.info(String.format("Training time: %fs\n",
        (double) (end - start) / 1000));

    double errorRate = 0;
    // calculate the error on test instance
    for (double[] testInstance : testInstances) {
      DoubleVector instance = new DenseDoubleVector(testInstance);
      double expected = instance.get(instance.getDimension() - 1);
      instance = instance.slice(instance.getDimension() - 1);
      double actual = ann.getOutput(instance).get(0);
      if (actual < 0.5 && expected >= 0.5 || actual >= 0.5 && expected < 0.5) {
        ++errorRate;
      }
    }
    errorRate /= testInstances.size();

    Log.info(String.format("Relative error: %f%%\n", errorRate * 100));
  }
  
  @Test
  public void testLogisticRegression() {
    this.testLogisticRegressionDistributedVersion();
    this.testLogisticRegressionDistributedVersionWithFeatureTransformer();
  }

  public void testLogisticRegressionDistributedVersion() {
    // write data into a sequence file
    String tmpStrDatasetPath = "/tmp/logistic_regression_data";
    Path tmpDatasetPath = new Path(tmpStrDatasetPath);
    String strDataPath = "src/test/resources/logistic_regression_data.txt";
    String modelPath = "/tmp/logistic-regression-distributed-model";

    Configuration conf = new Configuration();
    List<double[]> instanceList = new ArrayList<double[]>();
    List<double[]> trainingInstances = null;
    List<double[]> testInstances = null;

    try {
      FileSystem fs = FileSystem.get(new URI(tmpStrDatasetPath), conf);
      fs.delete(tmpDatasetPath, true);
      if (fs.exists(tmpDatasetPath)) {
        fs.createNewFile(tmpDatasetPath);
      }

      BufferedReader br = new BufferedReader(new FileReader(strDataPath));
      String line = null;
      int count = 0;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.trim().split(",");
        double[] instance = new double[tokens.length];
        for (int i = 0; i < tokens.length; ++i) {
          instance[i] = Double.parseDouble(tokens[i]);
        }
        instanceList.add(instance);
      }
      br.close();
      
      zeroOneNormalization(instanceList, instanceList.get(0).length - 1);
      
      // write training data to temporal sequence file
      SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
          tmpDatasetPath, LongWritable.class, VectorWritable.class);
      int testSize = 150;

      Collections.shuffle(instanceList);
      testInstances = new ArrayList<double[]>();
      testInstances.addAll(instanceList.subList(instanceList.size() - testSize,
          instanceList.size()));
      trainingInstances = instanceList.subList(0, instanceList.size()
          - testSize);

      for (double[] instance : trainingInstances) {
        DoubleVector vec = new DenseDoubleVector(instance);
        writer.append(new LongWritable(count++), new VectorWritable(vec));
      }
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    // create model
    int dimension = 8;
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.setLearningRate(0.7);
    ann.setMomemtumWeight(0.5);
    ann.setRegularizationWeight(0.1);
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("CrossEntropy"));
    ann.setModelPath(modelPath);

    long start = new Date().getTime();
    Map<String, String> trainingParameters = new HashMap<String, String>();
    trainingParameters.put("tasks", "5");
    trainingParameters.put("training.max.iterations", "2000");
    trainingParameters.put("training.batch.size", "300");
    trainingParameters.put("convergence.check.interval", "1000");
    ann.train(tmpDatasetPath, trainingParameters);

    long end = new Date().getTime();

    // validate results
    double errorRate = 0;
    // calculate the error on test instance
    for (double[] testInstance : testInstances) {
      DoubleVector instance = new DenseDoubleVector(testInstance);
      double expected = instance.get(instance.getDimension() - 1);
      instance = instance.slice(instance.getDimension() - 1);
      double actual = ann.getOutput(instance).get(0);
      if (actual < 0.5 && expected >= 0.5 || actual >= 0.5 && expected < 0.5) {
        ++errorRate;
      }
    }
    errorRate /= testInstances.size();

    Log.info(String.format("Training time: %fs\n",
        (double) (end - start) / 1000));
    Log.info(String.format("Relative error: %f%%\n", errorRate * 100));
  }
  
  public void testLogisticRegressionDistributedVersionWithFeatureTransformer() {
    // write data into a sequence file
    String tmpStrDatasetPath = "/tmp/logistic_regression_data_feature_transformer";
    Path tmpDatasetPath = new Path(tmpStrDatasetPath);
    String strDataPath = "src/test/resources/logistic_regression_data.txt";
    String modelPath = "/tmp/logistic-regression-distributed-model-feature-transformer";

    Configuration conf = new Configuration();
    List<double[]> instanceList = new ArrayList<double[]>();
    List<double[]> trainingInstances = null;
    List<double[]> testInstances = null;

    try {
      FileSystem fs = FileSystem.get(new URI(tmpStrDatasetPath), conf);
      fs.delete(tmpDatasetPath, true);
      if (fs.exists(tmpDatasetPath)) {
        fs.createNewFile(tmpDatasetPath);
      }

      BufferedReader br = new BufferedReader(new FileReader(strDataPath));
      String line = null;
      int count = 0;
      while ((line = br.readLine()) != null) {
        String[] tokens = line.trim().split(",");
        double[] instance = new double[tokens.length];
        for (int i = 0; i < tokens.length; ++i) {
          instance[i] = Double.parseDouble(tokens[i]);
        }
        instanceList.add(instance);
      }
      br.close();
      
      zeroOneNormalization(instanceList, instanceList.get(0).length - 1);
      
      // write training data to temporal sequence file
      SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
          tmpDatasetPath, LongWritable.class, VectorWritable.class);
      int testSize = 150;

      Collections.shuffle(instanceList);
      testInstances = new ArrayList<double[]>();
      testInstances.addAll(instanceList.subList(instanceList.size() - testSize,
          instanceList.size()));
      trainingInstances = instanceList.subList(0, instanceList.size()
          - testSize);

      for (double[] instance : trainingInstances) {
        DoubleVector vec = new DenseDoubleVector(instance);
        writer.append(new LongWritable(count++), new VectorWritable(vec));
      }
      writer.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    // create model
    int dimension = 8;
    SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
    ann.setLearningRate(0.7);
    ann.setMomemtumWeight(0.5);
    ann.setRegularizationWeight(0.1);
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(dimension, false,
        FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.addLayer(1, true, FunctionFactory.createDoubleFunction("Sigmoid"));
    ann.setCostFunction(FunctionFactory
        .createDoubleDoubleFunction("CrossEntropy"));
    ann.setModelPath(modelPath);
    
    FeatureTransformer featureTransformer = new DefaultFeatureTransformer();
    
    ann.setFeatureTransformer(featureTransformer);

    long start = new Date().getTime();
    Map<String, String> trainingParameters = new HashMap<String, String>();
    trainingParameters.put("tasks", "5");
    trainingParameters.put("training.max.iterations", "2000");
    trainingParameters.put("training.batch.size", "300");
    trainingParameters.put("convergence.check.interval", "1000");
    ann.train(tmpDatasetPath, trainingParameters);
    

    long end = new Date().getTime();

    // validate results
    double errorRate = 0;
    // calculate the error on test instance
    for (double[] testInstance : testInstances) {
      DoubleVector instance = new DenseDoubleVector(testInstance);
      double expected = instance.get(instance.getDimension() - 1);
      instance = instance.slice(instance.getDimension() - 1);
      instance = featureTransformer.transform(instance);
      double actual = ann.getOutput(instance).get(0);
      if (actual < 0.5 && expected >= 0.5 || actual >= 0.5 && expected < 0.5) {
        ++errorRate;
      }
    }
    errorRate /= testInstances.size();

    Log.info(String.format("Training time: %fs\n",
        (double) (end - start) / 1000));
    Log.info(String.format("Relative error: %f%%\n", errorRate * 100));
  }

}
