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
package org.apache.hama.ml.recommendation;

public interface RecommenderIO {
  /**
   * set preferences input data
   * @param path - file path to preferences data
   * 
   */
  void setInputPreferences(String path);
  
  /**
   * set user features input data
   * @param path - file path to user features data if any
   * 
   */
  void setInputUserFeatures(String path);
  
  /**
   * set item features input data
   * @param path - file path to item features data if any
   * 
   */
  void setInputItemFeatures(String path);

  /**
   * set output path for trained model
   * @param path - output path
   * 
   */
  void setOutputPath(String path);

}
