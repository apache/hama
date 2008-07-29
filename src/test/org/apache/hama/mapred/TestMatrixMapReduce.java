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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Vector;
import org.apache.hama.HamaTestCase;
import org.apache.hama.Matrix;
import org.apache.hama.io.VectorDatum;
import org.apache.log4j.Logger;

/**
 * Test Matrix Map/Reduce job
 */
public class TestMatrixMapReduce extends HamaTestCase {
  static final Logger LOG = Logger.getLogger(TestMatrixMapReduce.class);

  /** constructor */
  public TestMatrixMapReduce() {
    super();
  }

  public static class AdditionMap extends
      MatrixMap<ImmutableBytesWritable, VectorDatum> {
    protected Matrix B;
    public static final String MATRIX_B = "hama.addition.substraction.matrix.b";

    public void configure(JobConf job) {
      B = new Matrix(new HBaseConfiguration(), new Text("MatrixB"));
    }

    @Override
    public void map(ImmutableBytesWritable key, VectorDatum value,
        OutputCollector<ImmutableBytesWritable, VectorDatum> output,
        Reporter reporter) throws IOException {

      Vector v1 = new Vector(B.getRowResult(key.get()));
      Vector v2 = value.getVector();
      output.collect(key, v1.addition(key.get(), v2));
    }
  }

  public static class AdditionReduce extends
      MatrixReduce<ImmutableBytesWritable, VectorDatum> {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<VectorDatum> values,
        OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
        Reporter reporter) throws IOException {

      BatchUpdate b = new BatchUpdate(key.get());
      VectorDatum r = values.next();
      for (Map.Entry<byte[], Cell> f : r.entrySet()) {
        b.put(f.getKey(), f.getValue().getValue());
      }

      output.collect(key, b);
    }
  }

  public void testMatrixMapReduce() throws IOException {
    Matrix a = new Matrix(conf, new Text("MatrixA"));
    a.set(0, 0, 1);
    a.set(0, 1, 0);
    Matrix b = new Matrix(conf, new Text("MatrixB"));
    b.set(0, 0, 1);
    b.set(0, 1, 1);
    a.close();
    b.close();
    miniMRJob();
  }

  public void miniMRJob() throws IOException {
    Matrix c = new Matrix(conf, new Text("xanadu"));
    c.close();

    JobConf jobConf = new JobConf(conf, TestMatrixMapReduce.class);
    jobConf.setJobName("test MR job");

    MatrixMap.initJob("MatrixA", "column:", AdditionMap.class,
        ImmutableBytesWritable.class, VectorDatum.class, jobConf);
    MatrixReduce.initJob("xanadu", AdditionReduce.class, jobConf);

    jobConf.setNumMapTasks(1);
    jobConf.setNumReduceTasks(1);

    JobClient.runJob(jobConf);

    assertEquals(c.get(0, 0), 2.0);
    assertEquals(c.get(0, 1), 1.0);
  }

}
