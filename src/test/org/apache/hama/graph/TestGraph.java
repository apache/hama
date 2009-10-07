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
package org.apache.hama.graph;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.log4j.Logger;

public class TestGraph extends HamaCluster {
  static final Logger LOG = Logger.getLogger(TestGraph.class);
  private static HamaConfiguration conf;
  private static Graph adj;
  private static int[] result = new int[] { 4, 3, 2, 0, 1 };


  /**
   * @throws UnsupportedEncodingException
   */
  public TestGraph() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();
    conf = getConf();
    adj = new SparseGraph(conf);
  }

  public void testAddEdge() throws IOException {
    adj.addEdge(0, 1);
    adj.addEdge(0, 2);
    adj.addEdge(2, 3);
    adj.addEdge(3, 4);
    adj.addEdge(3, 6);
    adj.addEdge(4, 6);

    LocalBFSearcher bfs = new LocalBFSearcher(adj, 1);
    int[] path = bfs.path(4);

    for (int i = 0; i < path.length; i++) {
      assertEquals(result[i], path[i]);
    }
  }

  static class LocalBFSearcher {
    private Map<Integer, Integer> visited = new HashMap<Integer, Integer>();

    public LocalBFSearcher(Graph G, int s) throws IOException {
      Queue q = new Queue();
      q.enqueue(s);

      while (!q.isEmpty()) {
        int v = (Integer) q.dequeue();
        int[] neighbors = G.neighborsOf(v);
        for (int i = 0; i < neighbors.length; i++) {
          int w = neighbors[i];
          if (visited.get(w) == null) {
            q.enqueue(w);
            if (s != w)
              visited.put(w, v);
          }
        }
      }
    }

    public int pathLength(int v) {
      int len = -1;
      while (visited.get(v) != null) {
        v = (Integer) visited.get(v);
        len++;
      }
      return len;
    }

    public int[] path(int v) {
      int N = pathLength(v);
      int[] p = new int[N + 1];
      for (int i = 0; i <= N; i++) {
        p[i] = v;
        v = (Integer) visited.get(v);
      }
      return p;
    }
  }

  static class Queue {
    private Node first;
    private Node last;

    public boolean isEmpty() {
      return (first == null);
    }

    public void enqueue(Object anItem) {
      Node x = new Node();
      x.item = anItem;
      x.next = null;
      if (isEmpty())
        first = x;
      else
        last.next = x;
      last = x;
    }

    public Object dequeue() {
      Object val = first.item;
      first = first.next;
      return val;
    }
  }

  static class Node {
    Object item;
    Node next;
  }
}
