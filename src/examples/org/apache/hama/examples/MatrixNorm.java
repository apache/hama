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
import org.apache.hama.matrix.Matrix.Norm;

public class MatrixNorm extends AbstractExample {
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("norm [-m maps] [-r reduces] <matrix name> <type>");
      System.out.println("arguments: type - one | infinity | frobenius | maxvalue");
      System.exit(-1);
    } else {
      parseArgs(args);
    }

    HamaAdmin admin = new HamaAdminImpl(conf);
    Matrix a = admin.getMatrix(ARGS.get(0));
    if(ARGS.get(1).equalsIgnoreCase("one")) {
      System.out.println("The maximum absolute column sum of matrix '" + ARGS.get(0)
          + "' is " + a.norm(Norm.One));
    } else if(ARGS.get(1).equalsIgnoreCase("infinity")) {
      System.out.println("The maximum absolute row sum of matrix '" + ARGS.get(0)
          + "' is " + a.norm(Norm.Infinity));
    } else if(ARGS.get(1).equalsIgnoreCase("frobenius")) {
      System.out.println("The root of sum of squares of matrix '" + ARGS.get(0)
          + "' is " + a.norm(Norm.Frobenius));
    } else {
      System.out.println("The max absolute cell value of matrix '" + ARGS.get(0)
          + "' is " + a.norm(Norm.Maxvalue));
    }
  }
}
