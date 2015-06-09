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
package org.apache.hama.ml.classification;

/**
 * A {@link ClassifierAdapter} assigns a label of type <code>O</code> to inputs of type <code>I</code>
 * by the adapted implementation of a classification algorithm.
 *
 * NOTE: This is a simplistic adapter interface whose purpose is just to have a way of plugging in any
 * classification algorithm; therefore it's not meant to be a properly generic and flexible API for classification
 * but just an adapter.
 */
public interface ClassifierAdapter<I, O> {

  /**
   * assign a label (class) of type <code>O</code> to inputs of type <code>I</code>
   *
   * @param inputs the inputs as a single or array of <code>I</code>s
   * @return the label assigned of type <code>O</code>
   */
  public O assignClass(I... inputs);

  /**
   * train the underlying classifier with an adapted configuration and a dataset
   *
   * @param configurationAdapter the adapted configuration to train the underlying classifier
   * @param inputs               the dataset as a series of inputs of type <code>I</code>
   */
  public void train(ClassifierConfigurationAdapter<ClassifierAdapter<I,O>> configurationAdapter, I... inputs);

}
