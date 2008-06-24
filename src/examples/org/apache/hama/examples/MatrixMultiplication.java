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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hama.Matrix;

public class MatrixMultiplication {

  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("multiplication <map_num> <row_m> <column_n>");
      System.exit(-1);
    }

    int map = Integer.parseInt(args[0]);
    int row = Integer.parseInt(args[1]);
    int column = Integer.parseInt(args[2]);

    Configuration conf = new HBaseConfiguration();
    
    conf.setInt("mapred.map.tasks", map);
    conf.setInt("mapred.reduce.tasks", 1);

    Matrix a = Matrix.random(conf, row, column);
    System.out.println("Create the " + row + " * " + column
        + " random matrix A.");
    Matrix b = Matrix.random(conf, row, column);
    System.out.println("Create the " + row + " * " + column
        + " random matrix B.");

    long start = System.currentTimeMillis();
    Matrix c = a.multiply(b);
    long end = System.currentTimeMillis();

    System.out.println(executeTime(start, end));
    a.clear();
    b.clear();
    c.clear();
  }

  public static String executeTime(long start, long end) {
    return "(" + String.format("%.2f", Double.valueOf((end - start) * 0.001))
        + " sec)";
  }
}
