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

import java.util.List;

/**
 * Recommendation engine that is used to train with BSP. 
 * and predict user preferences.
 */
public interface Recommender {
  
  /**
   * train model,
   * default behavior is saving model after train
   * @return true if success
   */
  boolean train();

  /**
   * save model
   * @return true if success
   */
  boolean save();
  
  /**
   * load model from given path
   * @param path - path of saved model
   * @param lazy - some models are bigger than available memory,
   *               set this to {@value true} if memory is less than data model
   *               for faster prediction, set to {@value false} if using
   *               file access and reading from file is fine in prediction phase
   * @return true if success
   */
  boolean load(String path, boolean lazy);

  /**
   * 
   * @param userId
   * @param itemId
   * @return estimated preference value
   */
  double estimatePreference(long userId, long itemId);

  /**
   * get most preferred items for user
   * @param userId
   * @param count
   * @return most preferred items with their values
   */
  List<Preference<Long, Long>> getMostPreferredItems(long userId, int count); 
}

