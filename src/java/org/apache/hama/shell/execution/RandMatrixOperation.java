/*
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
package org.apache.hama.shell.execution;

import java.io.IOException;

import org.apache.hama.DenseMatrix;
import org.apache.hama.HamaConfiguration;

/**
 * Generate a random matrix.
 * 
 * A = Matrix.Random #mRows #mColumns
 *
 */
public class RandMatrixOperation extends HamaOperation {

  // rows, columns;
  int mRows, mColumns;
  
  public RandMatrixOperation(HamaConfiguration conf, int rows, int columns) {
    super(conf);
    this.mRows = rows;
    this.mColumns = columns;
  }
  
  public RandMatrixOperation(HamaConfiguration conf, int rows, int columns, 
      int map, int reduce) {
    super(conf, map, reduce);
    this.mRows = rows;
    this.mColumns = columns;
  }
  
  public Object operate() throws IOException {
    System.out.println("it is a RandMatrix operation, " +
    		"[rows="+mRows+"], [columns="+mColumns+"].");
    return DenseMatrix.random(conf, mRows, mColumns);
  }

}
