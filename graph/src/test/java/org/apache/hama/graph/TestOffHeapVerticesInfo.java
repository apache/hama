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
package org.apache.hama.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.graph.example.PageRank.PageRankVertex;
import org.junit.Test;

public class TestOffHeapVerticesInfo {

  @Test
  public void testOffHeapVerticesInfoLifeCycle() throws Exception {
    OffHeapVerticesInfo<Text, NullWritable, DoubleWritable> info = new OffHeapVerticesInfo<Text, NullWritable, DoubleWritable>();
    HamaConfiguration conf = new HamaConfiguration();
    conf.set(GraphJob.VERTEX_CLASS_ATTR, PageRankVertex.class.getName());
    conf.set(GraphJob.VERTEX_EDGE_VALUE_CLASS_ATTR,
        NullWritable.class.getName());
    conf.set(GraphJob.VERTEX_ID_CLASS_ATTR, Text.class.getName());
    conf.set(GraphJob.VERTEX_VALUE_CLASS_ATTR, DoubleWritable.class.getName());
    GraphJobRunner.<Text, NullWritable, DoubleWritable> initClasses(conf);
    TaskAttemptID attempt = new TaskAttemptID("123", 1, 1, 0);
    try {
      ArrayList<PageRankVertex> list = new ArrayList<PageRankVertex>();

      for (int i = 0; i < 10; i++) {
        PageRankVertex v = new PageRankVertex();
        v.setVertexID(new Text(i + ""));
        if (i % 2 == 0) {
          v.setValue(new DoubleWritable(i * 2));
        }
        v.addEdge(new Edge<Text, NullWritable>(new Text((10 - i) + ""), null));

        list.add(v);
      }

      info.init(null, conf, attempt);
      for (PageRankVertex v : list) {
        info.addVertex(v);
      }

      info.finishAdditions();

      assertEquals(10, info.size());
      // no we want to iterate and check if the result can properly be obtained

      int index = 0;
      IDSkippingIterator<Text, NullWritable, DoubleWritable> iterator = info
          .skippingIterator();
      while (iterator.hasNext()) {
        Vertex<Text, NullWritable, DoubleWritable> next = iterator.next();
        PageRankVertex pageRankVertex = list.get(index);
        assertEquals(pageRankVertex.getVertexID().toString(), next
            .getVertexID().toString());
        if (index % 2 == 0) {
          assertEquals((int) next.getValue().get(), index * 2);
        } else {
          assertNull(next.getValue());
        }
        assertEquals(next.isHalted(), false);
        // check edges
        List<Edge<Text, NullWritable>> edges = next.getEdges();
        assertEquals(1, edges.size());
        Edge<Text, NullWritable> edge = edges.get(0);
        assertEquals(pageRankVertex.getEdges().get(0).getDestinationVertexID()
            .toString(), edge.getDestinationVertexID().toString());
        assertNull(edge.getValue());

        index++;
      }
      assertEquals(index, list.size());
      info.finishSuperstep();
      // iterate again and compute so vertices change internally
      iterator = info.skippingIterator();
      info.startSuperstep();
      while (iterator.hasNext()) {
        Vertex<Text, NullWritable, DoubleWritable> next = iterator.next();
        // override everything with constant 2
        next.setValue(new DoubleWritable(2));
        if (Integer.parseInt(next.getVertexID().toString()) == 3) {
          next.voteToHalt();
        }
        info.finishVertexComputation(next);
      }
      info.finishSuperstep();
      assertEquals(index, list.size());

    } finally {
      info.cleanup(conf, attempt);
    }

  }

  @Test
  public void testAdditionWithDefaults() throws Exception {
    OffHeapVerticesInfo<Text, NullWritable, DoubleWritable> verticesInfo =
            new OffHeapVerticesInfo<Text, NullWritable, DoubleWritable>();
    HamaConfiguration conf = new HamaConfiguration();
    verticesInfo.init(null, conf, null);
    Vertex<Text, NullWritable, DoubleWritable> vertex = new PageRankVertex();
    vertex.setVertexID(new Text("some-id"));
    verticesInfo.addVertex(vertex);
    assertTrue("added vertex could not be found in the cache", verticesInfo.skippingIterator().hasNext());
  }

  @Test
  public void testMassiveAdditionWithDefaults() throws Exception {
    OffHeapVerticesInfo<Text, NullWritable, DoubleWritable> verticesInfo =
            new OffHeapVerticesInfo<Text, NullWritable, DoubleWritable>();
    HamaConfiguration conf = new HamaConfiguration();
    verticesInfo.init(null, conf, null);
    assertEquals("vertices info size should be 0 at startup", 0, verticesInfo.size());
    Random r = new Random();
    int i = 10000;
    for (int n = 0; n < i; n++) {
      Vertex<Text, NullWritable, DoubleWritable> vertex = new PageRankVertex();
      vertex.setVertexID(new Text(String.valueOf(r.nextInt())));
      vertex.setValue(new DoubleWritable(r.nextDouble()));
      verticesInfo.addVertex(vertex);
    }
    verticesInfo.finishAdditions();
    assertEquals("vertices info size is not correct", i, verticesInfo.size());
  }

}
