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
package org.apache.hama.ml.recommendation.cf;

import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleVector;

public class MovieLensConverter {
  public boolean convert(String inputPrefs, String inputItemFeatures, String outputPath) {

    InputConverter conv = new InputConverter();
    String preferencesPath = inputPrefs;
    String itemFeaturesPath = inputItemFeatures;

    conv.setInputPreferences(preferencesPath, 
        new KeyValueParser<Text, VectorWritable>() {
      public void parseLine(String ln) {
        String[] split = ln.split("::");
        key = new Text(split[0]);
        value = new VectorWritable();
        double[] values = new double[2];
        values[0] = Double.valueOf(split[1]);
        values[1] = Double.valueOf(split[2]);
        DenseDoubleVector dd = new DenseDoubleVector(values);
        value.set(dd);
      }
    });

    final HashMap<String, Integer> genreIndexes = new HashMap<String, Integer>();
    genreIndexes.put("action", 0);
    genreIndexes.put("animation", 1);
    genreIndexes.put("children", 2);
    genreIndexes.put("comedy", 3);
    genreIndexes.put("crime", 4);
    genreIndexes.put("documentary", 5);
    genreIndexes.put("drama", 6);
    genreIndexes.put("fantasy", 7);
    genreIndexes.put("film-noir", 8);
    genreIndexes.put("horror", 9);
    genreIndexes.put("musical", 10);
    genreIndexes.put("mystery", 11);
    genreIndexes.put("romance", 12);
    genreIndexes.put("sci-fi", 13);
    genreIndexes.put("thriller", 14);
    genreIndexes.put("war", 15);
    genreIndexes.put("western", 16);
    genreIndexes.put("imax", 17);
    genreIndexes.put("adventure", 18);

    conv.setInputItemFeatures(itemFeaturesPath, 
        new KeyValueParser<Text, VectorWritable>() {
      public void parseLine(String ln) {
        String[] split = ln.split("::");
        key = new Text(split[0]);
        String[] genres = split[2].toLowerCase().split("[|]");
        value = new VectorWritable();
        
        DenseDoubleVector values = new DenseDoubleVector(genreIndexes.size());
        for (int i=0; i<genres.length; i++) {
          Integer id = genreIndexes.get(genres[i]);
          if (id == null) {
            continue;
          }
          values.set(id, 1);
        }
        value.set(values);
      }
    });
    return conv.convert(outputPath);
  }
}
