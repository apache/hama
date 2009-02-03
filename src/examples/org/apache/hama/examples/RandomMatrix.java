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

import java.io.IOException;

import org.apache.hama.DenseMatrix;

public class RandomMatrix extends AbstractExample {

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.out
          .println("random  [-m maps] [-r reduces] <rows> <columns> <matrix_name>");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    int row = Integer.parseInt(ARGS.get(0));
    int column = Integer.parseInt(ARGS.get(1));

    DenseMatrix a = DenseMatrix.random_mapred(conf, row, column);
    a.save(ARGS.get(2));
  }
}
