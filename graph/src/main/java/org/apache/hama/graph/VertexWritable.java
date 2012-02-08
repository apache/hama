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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VertexWritable implements Writable,
    WritableComparable<VertexWritable> {

  public String name;
  public int weight;

  public VertexWritable() {
    super();
  }

  public VertexWritable(String name) {
    super();
    this.name = name;
    this.weight = 0;
  }
  
  public VertexWritable(int weight, String name) {
    super();
    this.name = name;
    this.weight = weight;
  }

  public String getName() {
    return name;
  }

  public int getWeight() {
    return weight;
  }

  @Override
  public String toString() {
    return getName();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = in.readUTF();
    this.weight = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(name);
    out.writeInt(weight);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    VertexWritable other = (VertexWritable) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  @Override
  public int compareTo(VertexWritable o) {
    VertexWritable that = (VertexWritable) o;
    return this.name.compareTo(that.name);
  }

}
