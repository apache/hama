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
import java.util.Arrays;
import java.util.List;

import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.junit.Test;
import org.mortbay.log.Log;

/**
 * Test the functionalities of LogisticRegression.
 * 
 */
public class TestLogisticRegression {

  @Test
  public void testLogisticRegressionLocal() {
    // read logistic regression data
    String filepath = "src/test/resources/logistic_regression_data.txt";
    List<double[]> instanceList = new ArrayList<double[]>();

    try {
      BufferedReader br = new BufferedReader(new FileReader(filepath));
      String line = null;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) { // ignore comments
          continue;
        }
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

    int dimension = instanceList.get(0).length - 1;

    // min-max normalization
    double[] mins = new double[dimension];
    double[] maxs = new double[dimension];
    Arrays.fill(mins, Double.MAX_VALUE);
    Arrays.fill(maxs, Double.MIN_VALUE);

    for (double[] instance : instanceList) {
      for (int i = 0; i < instance.length - 1; ++i) {
        if (mins[i] > instance[i]) {
          mins[i] = instance[i];
        }
        if (maxs[i] < instance[i]) {
          maxs[i] = instance[i];
        }
      }
    }

    for (double[] instance : instanceList) {
      for (int i = 0; i < instance.length - 1; ++i) {
        double range = maxs[i] - mins[i];
        if (range != 0) {
          instance[i] = (instance[i] - mins[i]) / range;
        }
      }
    }

    // divide dataset into training and testing
    List<double[]> testInstances = new ArrayList<double[]>();
    testInstances.addAll(instanceList.subList(instanceList.size() - 100,
        instanceList.size()));
    List<double[]> trainingInstances = instanceList.subList(0,
        instanceList.size() - 100);

    LogisticRegression regression = new LogisticRegression(dimension);
    regression.setLearningRate(0.2);
    regression.setMomemtumWeight(0.1);
    regression.setRegularizationWeight(0.1);
    int iterations = 1000;
    for (int i = 0; i < iterations; ++i) {
      for (double[] trainingInstance : trainingInstances) {
        regression.trainOnline(new DenseDoubleVector(trainingInstance));
      }
    }

    double errorRate = 0;
    // calculate the error on test instance
    for (double[] testInstance : testInstances) {
      DoubleVector instance = new DenseDoubleVector(testInstance);
      double expected = instance.get(instance.getDimension() - 1);
      DoubleVector features = instance.slice(instance.getDimension() - 1);
      double actual = regression.getOutput(features).get(0);
      if (actual < 0.5 && expected >= 0.5 || actual >= 0.5 && expected < 0.5) {
        ++errorRate;
      }

    }
    errorRate /= testInstances.size();

    Log.info(String.format("Relative error: %f%%\n", errorRate * 100));
  }

}
