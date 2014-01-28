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
import java.util.List;
import java.util.Map;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.FunctionFactory;
import org.apache.hama.examples.util.ParserUtil;
import org.apache.hama.ml.ann.SmallLayeredNeuralNetwork;

import com.google.common.io.Closeables;

/**
 * The example of using {@link SmallLayeredNeuralNetwork}, including the
 * training phase and labeling phase.
 */
public class NeuralNetwork {
  // either train or label
  private static String mode;

  // arguments for labeling
  private static String featureDataPath;
  private static String resultDataPath;
  private static String modelPath;

  // arguments for training
  private static String trainingDataPath;
  private static int featureDimension;
  private static int labelDimension;
  private static List<Integer> hiddenLayerDimension;
  private static int iterations;
  private static double learningRate;
  private static double momemtumWeight;
  private static double regularizationWeight;

  public static boolean parseArgs(String[] args) {
    DefaultOptionBuilder optionBuilder = new DefaultOptionBuilder();
    GroupBuilder groupBuilder = new GroupBuilder();
    ArgumentBuilder argumentBuilder = new ArgumentBuilder();

    // the feature data (unlabeled data) path argument
    Option featureDataPathOption = optionBuilder
        .withLongName("feature-data-path")
        .withShortName("fp")
        .withDescription("the path of the feature data (unlabeled data).")
        .withArgument(
            argumentBuilder.withName("path").withMinimum(1).withMaximum(1)
                .create()).withRequired(true).create();

    // the result data path argument
    Option resultDataPathOption = optionBuilder
        .withLongName("result-data-path")
        .withShortName("rp")
        .withDescription("the path to store the result.")
        .withArgument(
            argumentBuilder.withName("path").withMinimum(1).withMaximum(1)
                .create()).withRequired(true).create();

    // the path to store the model
    Option modelPathOption = optionBuilder
        .withLongName("model-data-path")
        .withShortName("mp")
        .withDescription("the path to store the trained model.")
        .withArgument(
            argumentBuilder.withName("path").withMinimum(1).withMaximum(1)
                .create()).withRequired(true).create();

    // the path of the training data
    Option trainingDataPathOption = optionBuilder
        .withLongName("training-data-path")
        .withShortName("tp")
        .withDescription("the path to store the trained model.")
        .withArgument(
            argumentBuilder.withName("path").withMinimum(1).withMaximum(1)
                .create()).withRequired(true).create();

    // the dimension of the features
    Option featureDimensionOption = optionBuilder
        .withLongName("feature dimension")
        .withShortName("fd")
        .withDescription("the dimension of the features.")
        .withArgument(
            argumentBuilder.withName("dimension").withMinimum(1).withMaximum(1)
                .create()).withRequired(true).create();

    // the dimension of the hidden layers, at most two hidden layers
    Option hiddenLayerOption = optionBuilder
        .withLongName("hidden layer dimension(s)")
        .withShortName("hd")
        .withDescription("the dimension of the hidden layer(s).")
        .withArgument(
            argumentBuilder.withName("dimension").withMinimum(0).withMaximum(2)
                .create()).withRequired(true).create();

    // the dimension of the labels
    Option labelDimensionOption = optionBuilder
        .withLongName("label dimension")
        .withShortName("ld")
        .withDescription("the dimension of the label(s).")
        .withArgument(
            argumentBuilder.withName("dimension").withMinimum(1).withMaximum(1)
                .create()).withRequired(true).create();

    // the number of iterations for training
    Option iterationOption = optionBuilder
        .withLongName("iterations")
        .withShortName("itr")
        .withDescription("the iterations for training.")
        .withArgument(
            argumentBuilder.withName("iterations").withMinimum(1)
                .withMaximum(1).withDefault(1000).create()).create();

    // the learning rate
    Option learningRateOption = optionBuilder
        .withLongName("learning-rate")
        .withShortName("l")
        .withDescription("the learning rate for training, default 0.1.")
        .withArgument(
            argumentBuilder.withName("learning-rate").withMinimum(1)
                .withMaximum(1).withDefault(0.1).create()).create();

    // the momemtum weight
    Option momentumWeightOption = optionBuilder
        .withLongName("momemtum-weight")
        .withShortName("m")
        .withDescription("the momemtum weight for training, default 0.1.")
        .withArgument(
            argumentBuilder.withName("momemtum weight").withMinimum(1)
                .withMaximum(1).withDefault(0.1).create()).create();

    // the regularization weight
    Option regularizationWeightOption = optionBuilder
        .withLongName("regularization-weight")
        .withShortName("r")
        .withDescription("the regularization weight for training, default 0.")
        .withArgument(
            argumentBuilder.withName("regularization weight").withMinimum(1)
                .withMaximum(1).withDefault(0).create()).create();

    // the parameters related to train mode
    Group trainModeGroup = groupBuilder.withOption(trainingDataPathOption)
        .withOption(modelPathOption).withOption(featureDimensionOption)
        .withOption(labelDimensionOption).withOption(hiddenLayerOption)
        .withOption(iterationOption).withOption(learningRateOption)
        .withOption(momentumWeightOption)
        .withOption(regularizationWeightOption).create();

    // the parameters related to label mode
    Group labelModeGroup = groupBuilder.withOption(modelPathOption)
        .withOption(featureDataPathOption).withOption(resultDataPathOption)
        .create();

    Option trainModeOption = optionBuilder.withLongName("train")
        .withShortName("train").withDescription("the train mode")
        .withChildren(trainModeGroup).create();

    Option labelModeOption = optionBuilder.withLongName("label")
        .withShortName("label").withChildren(labelModeGroup)
        .withDescription("the label mode").create();

    Group normalGroup = groupBuilder.withOption(trainModeOption)
        .withOption(labelModeOption).create();

    Parser parser = new Parser();
    parser.setGroup(normalGroup);
    parser.setHelpFormatter(new HelpFormatter());
    parser.setHelpTrigger("--help");
    CommandLine cli = parser.parseAndHelp(args);
    if (cli == null) {
      return false;
    }

    // get the arguments
    boolean hasTrainMode = cli.hasOption(trainModeOption);
    boolean hasLabelMode = cli.hasOption(labelModeOption);
    if (hasTrainMode && hasLabelMode) {
      return false;
    }

    mode = hasTrainMode ? "train" : "label";
    if (mode.equals("train")) {
      trainingDataPath = ParserUtil.getString(cli, trainingDataPathOption);
      modelPath = ParserUtil.getString(cli, modelPathOption);
      featureDimension = ParserUtil.getInteger(cli, featureDimensionOption);
      labelDimension = ParserUtil.getInteger(cli, labelDimensionOption);
      hiddenLayerDimension = ParserUtil.getInts(cli, hiddenLayerOption);
      iterations = ParserUtil.getInteger(cli, iterationOption);
      learningRate = ParserUtil.getDouble(cli, learningRateOption);
      momemtumWeight = ParserUtil.getDouble(cli, momentumWeightOption);
      regularizationWeight = ParserUtil.getDouble(cli,
          regularizationWeightOption);
    } else {
      featureDataPath = ParserUtil.getString(cli, featureDataPathOption);
      modelPath = ParserUtil.getString(cli, modelPathOption);
      resultDataPath = ParserUtil.getString(cli, resultDataPathOption);
    }

    return true;
  }

  public static void main(String[] args) throws Exception {
    if (parseArgs(args)) {
      if (mode.equals("label")) {
        HamaConfiguration conf = new HamaConfiguration();
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

        Closeables.close(br, true);
        Closeables.close(bw, true);
      } else { // train the model
        SmallLayeredNeuralNetwork ann = new SmallLayeredNeuralNetwork();
        ann.setLearningRate(learningRate);
        ann.setMomemtumWeight(momemtumWeight);
        ann.setRegularizationWeight(regularizationWeight);
        ann.addLayer(featureDimension, false,
            FunctionFactory.createDoubleFunction("Sigmoid"));
        if (hiddenLayerDimension != null) {
          for (int dimension : hiddenLayerDimension) {
            ann.addLayer(dimension, false,
                FunctionFactory.createDoubleFunction("Sigmoid"));
          }
        }
        ann.addLayer(labelDimension, true,
            FunctionFactory.createDoubleFunction("Sigmoid"));
        ann.setCostFunction(FunctionFactory
            .createDoubleDoubleFunction("CrossEntropy"));
        ann.setModelPath(modelPath);

        Map<String, String> trainingParameters = new HashMap<String, String>();
        trainingParameters.put("tasks", "5");
        trainingParameters.put("training.max.iterations", "" + iterations);
        trainingParameters.put("training.batch.size", "300");
        trainingParameters.put("convergence.check.interval", "1000");
        ann.train(new Path(trainingDataPath), trainingParameters);
      }
    }
  }

}
