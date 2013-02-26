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

import java.util.List;
import java.util.PriorityQueue;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestGraphJobMessage extends TestCase {

  @Test
  public void testPriorityQueue() {
    PriorityQueue<GraphJobMessage> prio = new PriorityQueue<GraphJobMessage>();
    prio.addAll(getMessages());

    GraphJobMessage poll = prio.poll();
    assertEquals(true, poll.isMapMessage());
    poll = prio.poll();
    assertEquals(true, poll.isVertexMessage());
    assertEquals("1", poll.getVertexId().toString());

    poll = prio.poll();
    assertEquals(true, poll.isVertexMessage());
    assertEquals("2", poll.getVertexId().toString());

    poll = prio.poll();
    assertEquals(true, poll.isVertexMessage());
    assertEquals("3", poll.getVertexId().toString());

    assertTrue(prio.isEmpty());
  }

  public List<GraphJobMessage> getMessages() {
    GraphJobMessage mapMsg = new GraphJobMessage(new MapWritable());
    GraphJobMessage vertexMsg1 = new GraphJobMessage(new Text("1"),
        new IntWritable());
    GraphJobMessage vertexMsg2 = new GraphJobMessage(new Text("2"),
        new IntWritable());
    GraphJobMessage vertexMsg3 = new GraphJobMessage(new Text("3"),
        new IntWritable());
    return Lists.newArrayList(mapMsg, vertexMsg1, vertexMsg2, vertexMsg3);
  }

}
