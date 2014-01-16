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

import org.apache.commons.math3.util.Pair;

public interface ItemSimilarity {
  /**
   * calculate similarity between two items
   * @param item1 - first item
   * @param item2 - second item
   * @return item similarity, 0 == similar item
   */
  double calculateItemSimilarity(long item1, long item2);

  /**
   * get most similar users
   * @param item - item id
   * @param count - number of similar items
   * @return list of similar item ids(key) and similarity(value)
   */
  List<Pair<Long, Double>> getMostSimilarItems(long item, int count);
}
