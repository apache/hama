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
package org.apache.hama.ml.kcore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.graph.GraphJobMessage;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexOutputWriter;

public class KCoreVertexWriter
    implements
    VertexOutputWriter<LongWritable, IntWritable, LongWritable, LongWritable, KCoreMessage> {

  @Override
  public void setup(Configuration conf) {

  }

  @Override
  public void write(
      Vertex<LongWritable, LongWritable, KCoreMessage> vertex,
      BSPPeer<Writable, Writable, LongWritable, IntWritable, GraphJobMessage> peer)
      throws IOException {
    peer.write(vertex.getVertexID(),
        new IntWritable(((KCoreVertex) vertex).getCore()));
  }
}
