/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.ml.recommendation;

import java.net.URI;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hama.ml.recommendation.cf.MovieLensConverter;
import org.apache.hama.ml.recommendation.cf.OnlineCF;
import org.apache.hama.ml.recommendation.cf.function.MeanAbsError;
import org.junit.Test;

public class TestOnlineCF extends TestCase{
  @SuppressWarnings({ "deprecation", "rawtypes", "unchecked" })
  @Test
  public void testOnlineCF() {
    Preference[] train_prefs = {
                        new Preference<Integer, Integer>(1, 1, 4),
                        new Preference<Integer, Integer>(1, 2, 2.5),
                        new Preference<Integer, Integer>(1, 3, 3.5),
                        new Preference<Integer, Integer>(1, 4, 1),
                        new Preference<Integer, Integer>(1, 5, 3.5),
                        new Preference<Integer, Integer>(2, 1, 4),
                        new Preference<Integer, Integer>(2, 2, 2.5),
                        new Preference<Integer, Integer>(2, 3, 3.5),
                        new Preference<Integer, Integer>(2, 4, 1),
                        new Preference<Integer, Integer>(2, 5, 3.5),
                        new Preference<Integer, Integer>(3, 1, 4),
                        new Preference<Integer, Integer>(3, 2, 2.5),
                        new Preference<Integer, Integer>(3, 3, 3.5)};
    Preference[] test_prefs = {
                        new Preference<Integer, Integer>(1, 3, 3.5),
                        new Preference<Integer, Integer>(2, 4, 1),
                        new Preference<Integer, Integer>(3, 4, 1),
                        new Preference<Integer, Integer>(3, 5, 3.5)
                      };
    
    Random rnd = new Random();
    Long num = Long.valueOf(rnd.nextInt(100000));
    String fileName = "onlinecf_train" + num.toString();
    String outputFileName = "onlinecf_model" + num.toString();
    
    Configuration fsConf = new Configuration();
    String strDataPath = "/tmp/" + fileName;
    String convertedFileName = "/tmp/converted_" + fileName;
    Path dataPath = new Path(strDataPath);
    
    try {
      URI uri = new URI(strDataPath);
      FileSystem fs = FileSystem.get(uri, fsConf);
      fs.delete(dataPath, true);
      FSDataOutputStream fileOut = fs.create(dataPath, true);

      StringBuilder str = new StringBuilder();
      for (Preference taste : train_prefs) {
        str.append(taste.getUserId());
        str.append("::");
        str.append(taste.getItemId());
        str.append("::");
        str.append(taste.getValue().get());
        str.append("\n");
      }
      fileOut.writeBytes(str.toString());
      fileOut.close();
      
      MovieLensConverter converter = new MovieLensConverter();
      assertEquals(true, converter.convert(strDataPath, null, convertedFileName));
      
      OnlineCF recommender = new OnlineCF();
      recommender.setInputPreferences(convertedFileName);
      recommender.setIteration(150);
      recommender.setMatrixRank(3);
      recommender.setSkipCount(1);
      recommender.setTaskCount(2);
      recommender.setUpdateFunction(MeanAbsError.class);
      recommender.setOutputPath(outputFileName);
      assertEquals(true, recommender.train());

      recommender.load(outputFileName, false);
      int correct = 0;
      for (Preference<Integer, Integer> test : test_prefs) {
        double actual = test.getValue().get();
        double estimated = recommender.estimatePreference(test.getUserId(), test.getItemId()); 
        correct += (Math.abs(actual-estimated)<0.5)?1:0;
      }

      assertEquals(test_prefs.length*0.75, correct, 1);

      fs.delete(new Path(outputFileName));
      fs.delete(new Path(strDataPath));
      fs.delete(new Path(convertedFileName));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
