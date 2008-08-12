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
package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hama.HamaTestCase;
import org.apache.hama.Matrix;
import org.apache.hama.Vector;
import org.apache.hama.algebra.AdditionMap;
import org.apache.hama.algebra.AdditionReduce;
import org.apache.log4j.Logger;

/**
 * Test Matrix Map/Reduce job
 */
public class TestMatrixMapReduce extends HamaTestCase {
  static final Logger LOG = Logger.getLogger(TestMatrixMapReduce.class);
  private String A = "matrixA";
  private String B = "matrixB";
  private String output = "output";

  /** constructor */
  public TestMatrixMapReduce() {
    super();
  }

  public void testMatrixMapReduce() throws IOException {
    Matrix matrixA = new Matrix(conf, A);
    matrixA.set(0, 0, 1);
    matrixA.set(0, 1, 0);

    Matrix matrixB = new Matrix(conf, B);
    matrixB.set(0, 0, 1);
    matrixB.set(0, 1, 1);

    miniMRJob();
  }

  public void miniMRJob() throws IOException {
    Matrix c = new Matrix(conf, output);

    JobConf jobConf = new JobConf(conf, TestMatrixMapReduce.class);
    jobConf.setJobName("test MR job");

    MatrixMap.initJob(A, B, AdditionMap.class, ImmutableBytesWritable.class,
        Vector.class, jobConf);
    MatrixReduce.initJob(output, AdditionReduce.class, jobConf);

    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(1);

    JobClient.runJob(jobConf);

    assertEquals(c.get(0, 0), 2.0);
    assertEquals(c.get(0, 1), 1.0);
  }

}
