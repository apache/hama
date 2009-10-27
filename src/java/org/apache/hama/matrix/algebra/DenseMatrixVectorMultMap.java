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
package org.apache.hama.matrix.algebra;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.util.BytesUtil;

public class DenseMatrixVectorMultMap extends
    TableMapper<IntWritable, MapWritable> implements Configurable {
  private Configuration conf = null;
  protected DenseVector currVector;
  public static final String ITH_ROW = "ith.row";
  public static final String MATRIX_A = "hama.multiplication.matrix.a";
  private IntWritable nKey = new IntWritable();

  public void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    double ithjth = currVector.get(BytesUtil.getRowIndex(key.get()));
    if (ithjth != 0) {
      DenseVector scaled = new DenseVector(value).scale(ithjth);
      context.write(nKey, scaled.getEntries());
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    DenseMatrix matrix_a;
    try {
      matrix_a = new DenseMatrix(new HamaConfiguration(conf), conf.get(MATRIX_A,
          ""));
      int ithRow = conf.getInt(ITH_ROW, 0);
      nKey.set(ithRow);
      currVector = matrix_a.getRow(ithRow);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
