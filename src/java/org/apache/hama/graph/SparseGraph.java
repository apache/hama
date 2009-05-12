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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.SparseMatrix;
import org.apache.hama.SparseVector;

/**
 * A implementation of a graph that is optimized to store edge sparse graphs
 */
public class SparseGraph implements Graph {
  protected HTable table;
  protected SparseMatrix adjMatrix;
  protected String name;

  /**
   * Construct the graph
   * 
   * @param conf
   * @throws IOException
   */
  public SparseGraph(HamaConfiguration conf) throws IOException {
    this.adjMatrix = new SparseMatrix(conf, Integer.MAX_VALUE, Integer.MAX_VALUE);
    this.name = adjMatrix.getPath();
    this.table = adjMatrix.getHTable();
  }

  /**
   * Initializes the graph from the given sparse matrix.
   * 
   * @param matrix
   * @throws IOException
   */
  public SparseGraph(SparseMatrix matrix) throws IOException {
    this.adjMatrix = matrix;
    this.name = matrix.getPath();
    this.table = matrix.getHTable();
  }

  /**
   * add v to w's list of neighbors and w to v's list of neighbors parallel
   * edges allowed
   */
  public void addEdge(int v, int w) throws IOException {
    adjMatrix.set(v, w, 1.0);
    adjMatrix.set(w, v, 1.0);
  }

  /**
   * Returns the degree of vertex v
   * 
   * @param v
   * @return the degree of vertex v
   * @throws IOException
   */
  public int getDegree(int v) throws IOException {
    SparseVector adjlist = this.adjMatrix.getRow(v);
    if (adjlist == null)
      return 0;
    else
      return adjlist.size();
  }

  /**
   * Returns the array of vertices incident to v
   * 
   * @param v
   * @return the array of vertices incident to v
   * @throws IOException
   */
  public int[] neighborsOf(int v) throws IOException {
    SparseVector adjlist = this.adjMatrix.getRow(v);
    Set<Writable> set = adjlist.getEntries().keySet();
    int[] arr = new int[set.size()];
    Iterator<Writable> it = set.iterator();
    int i = 0;
    while (it.hasNext()) {
      arr[i] = ((IntWritable) it.next()).get();
      i++;
    }

    return arr;
  }

  /**
   * for debugging only
   */
  public String toString() {
    Scanner scan;
    StringBuilder result = new StringBuilder();

    try {
      scan = table.getScanner(new String[] { Constants.COLUMN }, "");
      Iterator<RowResult> it = scan.iterator();
      while (it.hasNext()) {
        RowResult rs = it.next();
        result.append(new Text(rs.getRow()) + ": ");
        for (Map.Entry<byte[], Cell> e : rs.entrySet()) {
          result.append(new String(e.getKey()) + " ");
        }
        result.append("\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result.toString();
  }

  public String getName() {
    return this.name;
  }
}
