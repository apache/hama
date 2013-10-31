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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.junit.Test;
import org.mortbay.log.Log;

/**
 * Test the functionalities of the linear regression model.
 * 
 */
public class TestLinearRegression {

  @Test
  public void testLinearRegressionSimple() {
    // y = 2.1 * x_1 + 0.7 * x_2 * 0.1 * x_3
    double[][] instances = { { 1, 1, 1, 2.9 }, { 5, 2, 3, 12.2 },
        { 2, 5, 8, 8.5 }, { 0.5, 0.1, 0.2, 1.14 }, { 10, 20, 30, 38 },
        { 0.6, 20, 5, 16.76 } };

    LinearRegression regression = new LinearRegression(instances[0].length - 1);
    regression.setLearningRate(0.001);
    regression.setMomemtumWeight(0.1);

    int iterations = 100;
    for (int i = 0; i < iterations; ++i) {
      for (int j = 0; j < instances.length; ++j) {
        regression.trainOnline(new DenseDoubleVector(instances[j]));
      }
    }

    double relativeError = 0;
    for (int i = 0; i < instances.length; ++i) {
      DoubleVector test = new DenseDoubleVector(instances[i]);
      double expected = test.get(test.getDimension() - 1);
      test = test.slice(test.getDimension() - 1);
      double actual = regression.getOutput(test).get(0);
      relativeError += Math.abs((expected - actual) / expected);
    }

    relativeError /= instances.length;
    Log.info(String.format("Relative error %f%%\n", relativeError));
  }

  @Test
  public void testLinearRegressionOnlineTraining() {
    // read linear regression data
    String filepath = "src/test/resources/linear_regression_data.txt";
    List<double[]> instanceList = new ArrayList<double[]>();

    try {
      BufferedReader br = new BufferedReader(new FileReader(filepath));
      String line = null;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) { // ignore comments
          continue;
        }
        String[] tokens = line.trim().split(" ");
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
    // divide dataset into training and testing
    List<double[]> testInstances = new ArrayList<double[]>();
    testInstances.addAll(instanceList.subList(instanceList.size() - 20,
        instanceList.size()));
    List<double[]> trainingInstances = instanceList.subList(0,
        instanceList.size() - 20);

    int dimension = instanceList.get(0).length - 1;

    LinearRegression regression = new LinearRegression(dimension);
    regression.setLearningRate(0.00000005);
    regression.setMomemtumWeight(0.1);
    regression.setRegularizationWeight(0.05);
    int iterations = 2000;
    for (int i = 0; i < iterations; ++i) {
      for (double[] trainingInstance : trainingInstances) {
        regression.trainOnline(new DenseDoubleVector(trainingInstance));
      }
    }

    double relativeError = 0.0;
    // calculate the error on test instance
    for (double[] testInstance : testInstances) {
      DoubleVector instance = new DenseDoubleVector(testInstance);
      double expected = instance.get(instance.getDimension() - 1);
      instance = instance.slice(instance.getDimension() - 1);
      double actual = regression.getOutput(instance).get(0);
      if (expected == 0) {
        expected = 0.0000001;
      }
      relativeError += Math.abs((expected - actual) / expected);
    }
    relativeError /= testInstances.size();

    Log.info(String.format("Relative error: %f%%\n", relativeError * 100));
  }

}
