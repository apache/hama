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
package org.apache.hama.examples.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class ShortestPathVertex extends Vertex {

  private int weight;
  private int cost = Integer.MAX_VALUE;

  public ShortestPathVertex() {
  }

  public ShortestPathVertex(int weight, String name) {
    super(name);
    this.weight = weight;
  }

  public ShortestPathVertex(int weight, String name, int cost) {
    super(name);
    this.weight = weight;
    this.cost = cost;
  }

  public String getName() {
    return name;
  }

  public int getCost() {
    return cost;
  }

  public void setCost(Integer cost) {
    this.cost = cost;
  }

  public int getWeight() {
    return weight;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    weight = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(weight);
  }

}
