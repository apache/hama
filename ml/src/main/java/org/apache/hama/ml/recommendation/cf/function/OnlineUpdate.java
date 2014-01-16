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
package org.apache.hama.ml.recommendation.cf.function;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DoubleMatrix;

public class OnlineUpdate {
  public static class InputStructure {
    public VectorWritable user;
    public VectorWritable item;
    public VectorWritable userFeatures;
    public VectorWritable itemFeatures;
    public DoubleMatrix userFeatureFactorized;
    public DoubleMatrix  itemFeatureFactorized;
    public DoubleWritable expectedScore;
  }

  public static class OutputStructure {
    public VectorWritable userFactorized;
    public VectorWritable itemFactorized;
    public DoubleMatrix  userFeatureFactorized;
    public DoubleMatrix  itemFeatureFactorized;
  }

  public static interface Function {
    OnlineUpdate.OutputStructure compute(OnlineUpdate.InputStructure e);
    double predict(OnlineUpdate.InputStructure e);
  }
}
