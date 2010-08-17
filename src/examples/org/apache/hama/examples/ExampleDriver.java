/**
 * Copyright 2007 The Apache Software Foundation
 *
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

import org.apache.hadoop.util.ProgramDriver;

public class ExampleDriver {
  
  public static void main(String[] args) {
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("pi", PiEstimator.class, "Pi Estimator");
      pgd.addClass("rand", RandomMatrix.class, "Generate matrix with random elements.");
      pgd.addClass("mult", MatrixMultiplication.class, "Mat-Mat Multiplication.");
      pgd.addClass("similarity", CosineSimilarityMatrix.class, "Cosine Similarity Matrix.");
      pgd.addClass("norms", MatrixNorm.class, "Matrix Norms.");
      pgd.driver(args);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
