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

import org.apache.hama.examples.util.Generator;
import org.apache.hama.util.ProgramDriver;

public class ExampleDriver {

  public static void main(String[] args) {
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("pi", PiEstimator.class, "Pi Estimator");
      pgd.addClass("sssp", SSSP.class, "Single Shortest Path");
      pgd.addClass("mdstsearch", MindistSearch.class,
          "Mindist search / Connected Components");
      pgd.addClass("cmb", CombineExample.class, "Combine");
      pgd.addClass("bench", RandBench.class, "Random Benchmark");
      pgd.addClass("pagerank", PageRank.class, "PageRank");
      pgd.addClass("inlnkcount", InlinkCount.class, "InlinkCount");
      pgd.addClass("bipartite", BipartiteMatching.class, "Bipartite Matching");
      pgd.addClass("semi", SemiClusterJobDriver.class, "Semi Clustering");
      pgd.addClass("kmeans", Kmeans.class, "K-Means Clustering");
      pgd.addClass("gd", GradientDescentExample.class, "Gradient Descent");
      pgd.addClass("neuralnets", NeuralNetwork.class, "Neural Network classification");

      pgd.addClass("gen", Generator.class, "Random Data Generator Util");
      pgd.driver(args);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
}
