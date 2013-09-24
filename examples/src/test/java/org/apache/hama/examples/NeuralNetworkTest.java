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
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.writable.VectorWritable;
import org.junit.Test;

/**
 * Test the functionality of NeuralNetwork Example.
 * 
 */
public class NeuralNetworkTest {

  @Test
  public void testNeuralnetsLabeling() {
    this.testNeuralNetworkTraining();

    String dataPath = "src/test/resources/neuralnets_classification_test.txt";
    String modelPath = "tmp/neuralnets.model";
    String resultPath = "tmp/neuralnets.txt";
    String mode = "label";
    try {
      NeuralNetwork
          .main(new String[] { mode, modelPath, dataPath, resultPath });

      // compare results with ground-truth
      BufferedReader groundTruthReader = new BufferedReader(new FileReader(
          "src/test/resources/neuralnets_classification_label.txt"));
      List<Double> groundTruthList = new ArrayList<Double>();
      String line = null;
      while ((line = groundTruthReader.readLine()) != null) {
        groundTruthList.add(Double.parseDouble(line));
      }
      groundTruthReader.close();
      
      BufferedReader resultReader = new BufferedReader(new FileReader(resultPath));
      List<Double> resultList = new ArrayList<Double>();
      while ((line = resultReader.readLine()) != null) {
        resultList.add(Double.parseDouble(line));
      }
      resultReader.close();
      int total = resultList.size();
      double correct = 0;
      for (int i = 0; i < groundTruthList.size(); ++i) {
        double actual = resultList.get(i);
        double expected = groundTruthList.get(i);
        if (actual < 0.5 && expected < 0.5 || actual >= 0.5 && expected >= 0.5) {
          ++correct;
        }
      }
      System.out.printf("Precision: %f\n", correct / total);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void testNeuralNetworkTraining() {
    String mode = "train";
    String strTrainingDataPath = "src/test/resources/neuralnets_classification_training.txt";
    String strSequenceTrainingDataPath = "tmp/test-neuralnets.data";
    int featureDimension = 8;
    int labelDimension = 1;

    Path sequenceTrainingDataPath = new Path(strSequenceTrainingDataPath);
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
          sequenceTrainingDataPath, LongWritable.class, VectorWritable.class);
      BufferedReader br = new BufferedReader(
          new FileReader(strTrainingDataPath));
      String line = null;
      // convert the data in sequence file format
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split(",");
        double[] vals = new double[tokens.length];
        for (int i = 0; i < tokens.length; ++i) {
          vals[i] = Double.parseDouble(tokens[i]);
        }
        writer.append(new LongWritable(), new VectorWritable(
            new DenseDoubleVector(vals)));
      }
      writer.close();
      br.close();
    } catch (IOException e1) {
      e1.printStackTrace();
    }

    String modelPath = "tmp/neuralnets.model";
    try {
      NeuralNetwork.main(new String[] { mode, strSequenceTrainingDataPath,
          modelPath, "" + featureDimension, "" + labelDimension });
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
