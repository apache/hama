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
import org.apache.hama.Matrix;

public class MatrixMultiplication extends AbstractExample {
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("mult  [-m maps] [-r reduces] <matrix_A> <matrix_B> [blocks]");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    String matrixA = ARGS.get(0);
    String matrixB = ARGS.get(1);
    
    DenseMatrix a = new DenseMatrix(conf, matrixA, false);
    DenseMatrix b = new DenseMatrix(conf, matrixB, false);
    
    if(ARGS.size() > 2) {
      a.blocking_mapred(Integer.parseInt(ARGS.get(2)));
      b.blocking_mapred(Integer.parseInt(ARGS.get(2)));
    }
    
    Matrix c = a.mult(b);
    c.close();
  }
}
