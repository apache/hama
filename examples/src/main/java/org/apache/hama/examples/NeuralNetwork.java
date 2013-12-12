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
package org.apache.hama.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.ml.ann.SmallLayeredNeuralNetwork;

/**
 * The example of using {@link SmallLayeredNeuralNetwork}, including the
 * training phase and labeling phase.
 */
public class NeuralNetwork {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      printUsage();
      return;
    }
    String mode = args[0];
    if (mode.equalsIgnoreCase("label")) {
      if (args.length < 4) {
        printUsage();
        return;
      }
      HamaConfiguration conf = new HamaConfiguration();

      String featureDataPath = args[1];
      String resultDataPath = args[2];
      String modelPath = args[3];

      SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork(modelPath);

      // process data in streaming approach
      FileSystem fs = FileSystem.get(new URI(featureDataPath), conf);
      BufferedReader br = new BufferedReader(new InputStreamReader(
          fs.open(new Path(featureDataPath))));
      Path outputPath = new Path(resultDataPath);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
          fs.create(outputPath)));

      String line = null;

      while ((line = br.readLine()) != null) {
        if (line.trim().length() == 0) {
          continue;
        }
        String[] tokens = line.trim().split(",");
        double[] vals = new double[tokens.length];
        for (int i = 0; i < tokens.length; ++i) {
          vals[i] = Double.parseDouble(tokens[i]);
        }
        DoubleVector instance = new DenseDoubleVector(vals);
        DoubleVector result = ann.getOutput(instance);
        double[] arrResult = result.toArray();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arrResult.length; ++i) {
          sb.append(arrResult[i]);
          if (i != arrResult.length - 1) {
            sb.append(",");
          } else {
            sb.append("\n");
          }
        }
        bw.write(sb.toString());
      }

      br.close();
      bw.close();
    } else if (mode.equals("train")) {
      if (args.length < 5) {
        printUsage();
        return;
      }

      String trainingDataPath = args[1];
      String trainedModelPath = args[2];

      int featureDimension = Integer.parseInt(args[3]);
      int labelDimension = Integer.parseInt(args[4]);

      int iteration = 1000;
      double learningRate = 0.4;
      double momemtumWeight = 0.2;
      double regularizationWeight = 0.01;

      // parse parameters
      if (args.length >= 6) {
        try {
          iteration = Integer.parseInt(args[5]);
          System.out.printf("Iteration: %d\n", iteration);
        } catch (NumberFormatException e) {
          System.err
              .println("MAX_ITERATION format invalid. It should be a positive number.");
          return;
        }
      }
      if (args.length >= 7) {
        try {
          learningRate = Double.parseDouble(args[6]);
          System.out.printf("Learning rate: %f\n", learningRate);
        } catch (NumberFormatException e) {
          System.err
              .println("LEARNING_RATE format invalid. It should be a positive double in range (0, 1.0)");
          return;
        }
      }
      if (args.length >= 8) {
        try {
          momemtumWeight = Double.parseDouble(args[7]);
          System.out.printf("Momemtum weight: %f\n", momemtumWeight);
        } catch (NumberFormatException e) {
          System.err
              .println("MOMEMTUM_WEIGHT format invalid. It should be a positive double in range (0, 1.0)");
          return;
        }
      }
      if (args.length >= 9) {
        try {
          regularizationWeight = Double.parseDouble(args[8]);
          System.out
              .printf("Regularization weight: %f\n", regularizationWeight);
        } catch (NumberFormatException e) {
          System.err
              .println("REGULARIZATION_WEIGHT format invalid. It should be a positive double in range (0, 1.0)");
          return;
        }
      }

      // train the model
      SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
      ann.setLearningRate(learningRate);
      ann.setMomemtumWeight(momemtumWeight);
      ann.setRegularizationWeight(regularizationWeight);
      ann.addLayer(featureDimension, false,
          FunctionFactory.createDoubleFunction("Sigmoid"));
      ann.addLayer(featureDimension, false,
          FunctionFactory.createDoubleFunction("Sigmoid"));
      ann.addLayer(labelDimension, true,
          FunctionFactory.createDoubleFunction("Sigmoid"));
      ann.setCostFunction(FunctionFactory
          .createDoubleDoubleFunction("CrossEntropy"));
      ann.setModelPath(trainedModelPath);

      Map<String, String> trainingParameters = new HashMap<String, String>();
      trainingParameters.put("tasks", "5");
      trainingParameters.put("training.max.iterations", "" + iteration);
      trainingParameters.put("training.batch.size", "300");
      trainingParameters.put("convergence.check.interval", "1000");
      ann.train(new Path(trainingDataPath), trainingParameters);
    }

  }

  private static void printUsage() {
    System.out
        .println("USAGE: <MODE> <INPUT_PATH> <OUTPUT_PATH> <MODEL_PATH>|<FEATURE_DIMENSION> <LABEL_DIMENSION> [<MAX_ITERATION> <LEARNING_RATE> <MOMEMTUM_WEIGHT> <REGULARIZATION_WEIGHT>]");
    System.out
        .println("\tMODE\t- train: train the model with given training data.");
    System.out
        .println("\t\t- label: obtain the result by feeding the features to the neural network.");
    System.out
        .println("\tINPUT_PATH\tin 'train' mode, it is the path of the training data; in 'label' mode, it is the path of the to be evaluated data that lacks the label.");
    System.out
        .println("\tOUTPUT_PATH\tin 'train' mode, it is where the trained model is stored; in 'label' mode, it is where the labeled data is stored.");
    System.out.println("\n\tConditional Parameters:");
    System.out
        .println("\tMODEL_PATH\tonly required in 'label' mode. It specifies where to load the trained neural network model.");
    System.out
        .println("\tMAX_ITERATION\tonly used in 'train' mode. It specifies how many iterations for the neural network to run. Default is 0.01.");
    System.out
        .println("\tLEARNING_RATE\tonly used to 'train' mode. It specifies the degree of aggregation for learning, usually in range (0, 1.0). Default is 0.1.");
    System.out
        .println("\tMOMEMTUM_WEIGHT\tonly used to 'train' mode. It specifies the weight of momemtum. Default is 0.");
    System.out
        .println("\tREGULARIZATION_WEIGHT\tonly required in 'train' model. It specifies the weight of reqularization.");
    System.out.println("\nExample:");
    System.out
        .println("Train a neural network with with feature dimension 8, label dimension 1 and default setting:\n\tneuralnets train hdfs://localhost:30002/training_data hdfs://localhost:30002/model 8 1");
    System.out
        .println("Train a neural network with with feature dimension 8, label dimension 1 and specify learning rate as 0.1, momemtum rate as 0.2, and regularization weight as 0.01:\n\tneuralnets.train hdfs://localhost:30002/training_data hdfs://localhost:30002/model 8 1 0.1 0.2 0.01");
    System.out
        .println("Label the data with trained model:\n\tneuralnets evaluate hdfs://localhost:30002/unlabeled_data hdfs://localhost:30002/result hdfs://localhost:30002/model");
  }

}
