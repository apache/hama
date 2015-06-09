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
package org.apache.hama.ml.recommendation.cf;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.commons.io.VectorWritable;

/**
 * 
 * Helper class to convert input data
 * into OnlineCF compatible format
 * set inputs and parse line by line.
 *
 */
public class InputConverter {
  private String inputPreferences = null;
  private String inputUserFeatures = null;
  private String inputItemFeatures = null;
  private KeyValueParser<Text, VectorWritable> preferencesParser = null;
  private KeyValueParser<Text, VectorWritable> userFeatureParser = null;
  private KeyValueParser<Text, VectorWritable> itemFeatureParser = null;
  
  /**
   * set preferences input and related parser line by line
   * @param path - path for preferences input data
   * @param parser - line by line parser of input data
   */
  public void setInputPreferences(String path, KeyValueParser<Text, VectorWritable> parser) {
    inputPreferences = path;
    preferencesParser = parser;
  }
  
  /**
   * set user features input and related parser line by line
   * @param path - path for user features input data
   * @param parser - line by line parser of input data
   */
  public void setInputUserFeatures(String path, KeyValueParser<Text, VectorWritable> parser) {
    inputUserFeatures = path;
    userFeatureParser = parser;
  }
  
  /**
   * set item features input and related parser line by line
   * @param path - path for item features input data
   * @param parser - line by line parser of input data
   */
  public void setInputItemFeatures(String path, KeyValueParser<Text, VectorWritable> parser) {
    inputItemFeatures = path;
    itemFeatureParser = parser;
  }

  
  /**
   * converting given inputs into compatible output and save
   * @param outputPath - output path of converted input
   * @return true if success
   */
  public boolean convert(String outputPath) {
    try {
      Configuration conf = new Configuration();
      //URI outputUri = new URI(outputPath);
      Path outputDataPath = new Path(outputPath);
      FileSystem fs = FileSystem.get(conf);
      fs.delete(outputDataPath, true);
      fs.createNewFile(outputDataPath);
      SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
          outputDataPath, Text.class, VectorWritable.class);

      // inputPreferences
      writeToFile(inputPreferences, OnlineCF.Settings.DFLT_PREFERENCE_DELIM, 
          preferencesParser, fs, writer);
      
      // user features
      writeToFile(inputUserFeatures, OnlineCF.Settings.DFLT_USER_DELIM, 
          userFeatureParser, fs, writer);

      // item features
      writeToFile(inputItemFeatures, OnlineCF.Settings.DFLT_ITEM_DELIM, 
          itemFeatureParser, fs, writer);

      writer.close();
      return true;

    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @SuppressWarnings("deprecation")
  /**
   * convert line by line and save
   * @param inputPath - path of file to read
   * @param defaultDelim - delimiter when writing to file as (delim_key, value)
   * @param parser - line by line parser of input data
   * @param fs - file system object
   * @param writer - output file writer
   * @throws IOException
   */
  private void writeToFile(
      String inputPath,
      String defaultDelim,
      KeyValueParser<Text, VectorWritable> parser,
      FileSystem fs,
      SequenceFile.Writer writer) throws IOException {
    if (inputPath == null || parser == null) {
      return ;
    }
    Path path = new Path(inputPath);
    FSDataInputStream preferencesIn = fs.open(path);
    String line = null;
    while ((line = preferencesIn.readLine()) != null) {
      parser.parseLine(line);
      writer.append(new Text(defaultDelim + parser.getKey().toString()), 
          parser.getValue());
    }
  }
}
