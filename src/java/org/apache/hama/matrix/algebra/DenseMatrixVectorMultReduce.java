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
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.util.BytesUtil;

public class DenseMatrixVectorMultReduce extends
    TableReducer<IntWritable, MapWritable, Writable> {

  @Override
  public void reduce(IntWritable key, Iterable<MapWritable> values,
      Context context) throws IOException, InterruptedException {
    DenseVector sum = new DenseVector();

    for (MapWritable value : values) {
      DenseVector nVector = new DenseVector(value);

      if (sum.size() == 0) {
        sum.zeroFill(nVector.size());
        sum.add(nVector);
      } else {
        sum.add(nVector);
      }
    }

    Put put = new Put(BytesUtil.getRowIndex(key.get()));
    for (Map.Entry<Writable, Writable> e : sum.getEntries().entrySet()) {
      put.add(Constants.COLUMNFAMILY, Bytes.toBytes(String
          .valueOf(((IntWritable) e.getKey()).get())), Bytes
          .toBytes(((DoubleWritable) e.getValue()).get()));
    }

    context.write(new ImmutableBytesWritable(BytesUtil.getRowIndex(key.get())),
        put);
  }
}
