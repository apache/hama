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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * VertexInputFormat for the KCore algorithm specified in tab-delimited text
 * file format.
 */
public class KCoreVertexReader
    extends
    VertexInputReader<LongWritable, Text, LongWritable, LongWritable, KCoreMessage> {

  @Override
  public boolean parseVertex(LongWritable key, Text value,
      Vertex<LongWritable, LongWritable, KCoreMessage> vertex) throws Exception {
    String[] vertices = value.toString().split("\t");
    List<Edge<LongWritable, LongWritable>> edges = new ArrayList<Edge<LongWritable, LongWritable>>();

    for (int i = 1; i < vertices.length; i++) {
      LongWritable destID = new LongWritable(Long.parseLong(vertices[i]));
      edges.add(new Edge<LongWritable, LongWritable>(destID,
          new LongWritable(0)));
    }

    vertex.setEdges(edges);
    vertex.setValue(new KCoreMessage());
    vertex.setVertexID(new LongWritable(Long.parseLong(vertices[0])));
    return true;
  }
}
