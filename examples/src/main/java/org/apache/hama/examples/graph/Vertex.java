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

import org.apache.hama.examples.graph.partitioning.PartitionableWritable;

public class Vertex implements PartitionableWritable {

  protected int id;
  protected String name;

  public Vertex() {
    super();
  }

  public Vertex(String name) {
    super();
    this.name = name;
    this.id = name.hashCode();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = in.readInt();
    this.name = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeUTF(name);
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Vertex other = (Vertex) obj;
    if (!name.equals(other.name))
      return false;
    return true;
  }

  @Override
  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

}
