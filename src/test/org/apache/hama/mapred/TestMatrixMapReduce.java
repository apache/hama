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
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.HamaTestCase;
import org.apache.hama.Matrix;
import org.apache.hama.algebra.AdditionSubtractionReduce;
import org.apache.log4j.Logger;

/**
 * Test Matrix Map/Reduce job
 */
public class TestMatrixMapReduce extends HamaTestCase {
  static final Logger LOG = Logger.getLogger(TestMatrixMapReduce.class);
  private Matrix a;
  private Matrix b;
  private Matrix c;

  /** constructor */
  public TestMatrixMapReduce() {
    super();
  }

  public static class AdditionMap extends MatrixMap<Text, MapWritable> {
    protected Matrix matrix_b;
    public static final String MATRIX_B = "hama.addition.matrix.b";

    public void configure(JobConf job) {
      Text b_name = new Text(job.get(MATRIX_B, ""));
      matrix_b = new Matrix(new HBaseConfiguration(), b_name);
    }

    public static void initJob(String matrix_a, String matrix_b,
        Class<AdditionMap> map, JobConf jobConf) {
      initJob(matrix_a, map, jobConf);
      jobConf.set(MATRIX_B, matrix_b);
    }

    @Override
    public void map(HStoreKey key, MapWritable value,
        OutputCollector<Text, MapWritable> output, Reporter reporter)
        throws IOException {
      Text tKey = key.getRow();
      MapWritable sum = new MapWritable();

      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        double a = getDouble(e.getValue());
        double b = matrix_b.get(Integer.parseInt(tKey.toString()),
            getIndex((Text) e.getKey()));
        byte[] result = null;
        result = toBytes(a + b);
        sum.put(e.getKey(), new ImmutableBytesWritable(result));
      }
      output.collect(tKey, sum);
    }
  }

  public void testMatrixMapReduce() throws IOException {
    a = Matrix.random(conf, SIZE, SIZE);
    b = Matrix.random(conf, SIZE, SIZE);
    c = new Matrix(conf, new Text("matrixC"));
    miniMRJob();
  }

  public void miniMRJob() throws IOException {
      JobConf jobConf = new JobConf(conf, TestMatrixMapReduce.class);
      jobConf.setJobName("test MR job");
      jobConf.setInputFormat(MatrixInputFormat.class);
      jobConf.setOutputFormat(MatrixOutputFormat.class);
      AdditionMap.initJob(a.getName(), b.getName(), AdditionMap.class, jobConf);
      AdditionSubtractionReduce.initJob("matrixC",
          AdditionSubtractionReduce.class, jobConf);

      jobConf.setNumMapTasks(1);
      jobConf.setNumReduceTasks(1);

      JobClient.runJob(jobConf);
      
      assertEquals(c.getRowDimension(), SIZE);
      assertEquals(c.getColumnDimension(), SIZE);
      
      for(int i = 0; i < c.getRowDimension(); i++) {
        for(int j = 0; j < c.getColumnDimension(); j++) {
          assertEquals(c.get(i, j), (a.get(i, j) + b.get(i, j))); 
        }
      }
  }
}
