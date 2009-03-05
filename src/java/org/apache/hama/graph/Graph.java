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

/**
 * Basic graph interface.
 */
public interface Graph {

  /**
   * Adds an edge from v to w
   * 
   * @param v
   * @param w
   * @throws IOException
   */
  public void addEdge(int v, int w) throws IOException;

  /**
   * Return the degree of vertex v
   * 
   * @param v
   * @return the degree of vertex v
   * @throws IOException
   */
  public int getDegree(int v) throws IOException;

  /**
   * Return the array of vertices incident to v
   * 
   * @param v
   * @return the array of vertices incident to v
   * @throws IOException
   */
  public int[] neighborsOf(int v) throws IOException;

}
