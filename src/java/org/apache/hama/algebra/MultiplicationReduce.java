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
package org.apache.hama.algebra;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.mapred.MatrixReduce;

/**
 * Accumulate the multiplied values.
 */
public class MultiplicationReduce extends MatrixReduce<Text, MapWritable> {

  @Override
  public void reduce(Text key, Iterator<MapWritable> values,
      OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException {
    Map<String, Double> sum = new HashMap<String, Double>();

    while (values.hasNext()) {
      for (Map.Entry<Writable, Writable> e : values.next().entrySet()) {
        String sKey = e.getKey().toString();
        if (sum.containsKey(sKey)) {
          double before = sum.get(e.getKey().toString());
          sum.put(sKey, before + getDouble(e.getValue()));
        } else {
          sum.put(sKey, getDouble(e.getValue()));
        }
      }
    }

    MapWritable value = new MapWritable();
    for (Map.Entry<String, Double> f : sum.entrySet()) {
      Writable val = getBytesWritable(f.getValue());
      value.put(new Text(f.getKey()), val);
    }
    output.collect(key, value);
  }
}
