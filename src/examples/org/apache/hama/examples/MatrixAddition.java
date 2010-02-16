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

import org.apache.hama.HamaAdmin;
import org.apache.hama.HamaAdminImpl;
import org.apache.hama.matrix.Matrix;

public class MatrixAddition extends AbstractExample {
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out
          .println("add [-m maps] [-r reduces] <matrix A> <matrix B> [alpha]");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    String matrixA = ARGS.get(0);
    String matrixB = ARGS.get(1);

    HamaAdmin admin = new HamaAdminImpl(conf);
    Matrix a = admin.getMatrix(matrixA);
    Matrix b = admin.getMatrix(matrixB);

    Matrix c;
    if (a.getType().equals("SparseMatrix")) {
      System.out
          .println("The addition of sparse matrices is not implemented yet.");
    } else {
      if (ARGS.size() > 2) {
        System.out.println("C = " + Double.parseDouble(ARGS.get(2))
            + " * B + A");
        c = a.add(Double.parseDouble(ARGS.get(2)), b);
      } else {
        System.out.println("C = A + B");
        c = a.add(b);
      }

      c.close();
    }

  }
}
