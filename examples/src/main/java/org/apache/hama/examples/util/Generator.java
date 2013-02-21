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
package org.apache.hama.examples.util;

public class Generator {

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Valid command names are:");
      System.out
          .println("  symmetric: Generate random symmetric matrix, which can be used as a input of graph examples.");
      System.out
          .println("  fastgen: Generate random matrix, which can be used as a input of graph examples and is faster than symmetric.");
      System.out.println("  square: Generate random square matrix.");
      System.exit(1);
    }

    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);

    if (args[0].equals("symmetric")) {
      SymmetricMatrixGen.main(newArgs);
    } else if (args[0].equals("fastgen")) {
      FastGraphGen.main(newArgs);
    } else if (args[0].equals("square")) {
      System.out.println("Not implemented yet.");
      // SquareMatrixGen.main(newArgs);
    }
  }
}
